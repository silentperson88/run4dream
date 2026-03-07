const { pool } = require("../config/db");
const { hashPassword, comparePassword } = require("../utils/password");
const { sendEmail } = require("../services/email.service");
const { otpTemplate, welcomeTemplate } = require("../utils/email/templates");
const { signToken } = require("../utils/jwt");
const { response } = require("../utils/response.utils");

const OTP_EXPIRY_HOURS = 24;
const OTP_RESEND_COOLDOWN_MS = 60 * 1000;

function generateOtp() {
  return String(Math.floor(100000 + Math.random() * 900000));
}

function ensureCooldown(lastSentAt) {
  if (!lastSentAt) return true;
  return Date.now() - new Date(lastSentAt).getTime() >= OTP_RESEND_COOLDOWN_MS;
}

const USER_RESPONSE_SELECT = `
  id, email, role, wallet_fund, total_fund_added, total_fund_withdrawn, is_email_verified
`;

const toApiUser = (row) => ({
  id: row.id,
  email: row.email,
  role: row.role,
  wallet_fund: Number(row.wallet_fund || 0),
  total_fund_added: Number(row.total_fund_added || 0),
  total_fund_withdrawn: Number(row.total_fund_withdrawn || 0),
  isEmailVerified: !!row.is_email_verified,
});

const getUserByEmail = async (client, email, columns, { forUpdate = false } = {}) => {
  const sql = `
    SELECT ${columns}
    FROM users
    WHERE email = $1
    LIMIT 1
    ${forUpdate ? "FOR UPDATE" : ""}
  `;
  const { rows } = await client.query(sql, [email]);
  return rows[0] || null;
};

exports.register = async (req, res) => {
  const client = await pool.connect();
  try {
    const { name, email, password } = req.body;

    await client.query("BEGIN");

    const existingUser = await getUserByEmail(
      client,
      email,
      "id, name, email, is_email_verified, email_otp_last_sent_at",
      { forUpdate: true },
    );

    if (existingUser) {
      if (!existingUser.is_email_verified) {
        if (!ensureCooldown(existingUser.email_otp_last_sent_at)) {
          await client.query("COMMIT");
          return response(res, 200, "Verification OTP already sent", {
            isEmailVerified: false,
          });
        }

        const otp = generateOtp();
        const otpHash = await hashPassword(otp);

        await client.query(
          `
            UPDATE users
            SET
              email_otp_code_hash = $1,
              email_otp_purpose = 'VERIFY_EMAIL',
              email_otp_expires_at = NOW() + INTERVAL '${OTP_EXPIRY_HOURS} hour',
              email_otp_last_sent_at = NOW(),
              updated_at = NOW()
            WHERE id = $2
          `,
          [otpHash, existingUser.id],
        );

        await client.query("COMMIT");

        await sendEmail({
          to: existingUser.email,
          subject: "Verify your email",
          html: otpTemplate({
            name: existingUser.name,
            otp,
            purpose: "VERIFY_EMAIL",
          }),
          text: `Your OTP is ${otp}. It is valid for 24 hours.`,
        });

        return response(res, 200, "Verification OTP sent to your email", {
          isEmailVerified: false,
        });
      }

      await client.query("ROLLBACK");
      return response(res, 400, "Email already exists");
    }

    const passwordHash = await hashPassword(password);
    const otp = generateOtp();
    const otpHash = await hashPassword(otp);

    const insertRes = await client.query(
      `
        INSERT INTO users (
          name, email, password_hash, role, is_active, is_email_verified,
          wallet_fund, total_fund_added, total_fund_withdrawn, wallet_ledger,
          email_otp_code_hash, email_otp_purpose, email_otp_expires_at, email_otp_last_sent_at
        )
        VALUES (
          $1, $2, $3, 'USER', TRUE, FALSE,
          0, 0, 0, '[]'::jsonb,
          $4, 'VERIFY_EMAIL', NOW() + INTERVAL '${OTP_EXPIRY_HOURS} hour', NOW()
        )
        RETURNING ${USER_RESPONSE_SELECT}, name
      `,
      [name, email, passwordHash, otpHash],
    );
    const user = insertRes.rows[0];

    await client.query("COMMIT");

    await sendEmail({
      to: user.email,
      subject: "Verify your email",
      html: otpTemplate({
        name: user.name,
        otp,
        purpose: "VERIFY_EMAIL",
      }),
      text: `Your OTP is ${otp}. It is valid for 24 hours.`,
    });

    const token = signToken({
      userId: user.id,
      role: user.role,
    });

    return response(res, 201, "Registration successful", {
      token,
      user: toApiUser(user),
    });
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {}

    if (err.code === "23505") {
      return response(res, 400, "email already exists");
    }

    return response(res, 500, "Registration failed", err);
  } finally {
    client.release();
  }
};

exports.login = async (req, res) => {
  const client = await pool.connect();
  try {
    const { email, password } = req.body;
    const user = await getUserByEmail(
      client,
      email,
      "id, email, role, is_active, is_email_verified, password_hash, wallet_fund, total_fund_added, total_fund_withdrawn",
    );

    if (!user) return response(res, 400, "Invalid credentials");

    const match = await comparePassword(password, user.password_hash || "");
    if (!match) return response(res, 400, "Invalid credentials");
    if (!user.is_email_verified) return response(res, 403, "Email not verified");
    if (!user.is_active) return response(res, 403, "User is inactive");

    const token = signToken({
      userId: user.id,
      role: user.role,
    });

    return response(res, 200, "Login successful", {
      token,
      user: toApiUser(user),
    });
  } catch (err) {
    return response(res, 500, "Login failed", err);
  } finally {
    client.release();
  }
};

exports.verifyEmail = async (req, res) => {
  const client = await pool.connect();
  try {
    const { email, otp } = req.body;

    await client.query("BEGIN");

    const user = await getUserByEmail(
      client,
      email,
      "id, name, email, is_email_verified, email_otp_code_hash, email_otp_purpose, email_otp_expires_at",
      { forUpdate: true },
    );

    if (!user) {
      await client.query("ROLLBACK");
      return response(res, 400, "Invalid email or OTP");
    }

    if (user.is_email_verified) {
      await client.query("COMMIT");
      return response(res, 200, "Email already verified");
    }

    if (
      !user.email_otp_code_hash ||
      user.email_otp_purpose !== "VERIFY_EMAIL" ||
      !user.email_otp_expires_at ||
      new Date(user.email_otp_expires_at) < new Date()
    ) {
      await client.query("ROLLBACK");
      return response(res, 400, "OTP expired or invalid");
    }

    const match = await comparePassword(otp, user.email_otp_code_hash);
    if (!match) {
      await client.query("ROLLBACK");
      return response(res, 400, "Invalid email or OTP");
    }

    await client.query(
      `
        UPDATE users
        SET
          is_email_verified = TRUE,
          email_verified_at = NOW(),
          email_otp_code_hash = NULL,
          email_otp_purpose = NULL,
          email_otp_expires_at = NULL,
          email_otp_last_sent_at = NULL,
          updated_at = NOW()
        WHERE id = $1
      `,
      [user.id],
    );

    await client.query("COMMIT");

    await sendEmail({
      to: user.email,
      subject: "Welcome to Paper Trading",
      html: welcomeTemplate({ name: user.name }),
      text: `Welcome to Paper Trading, ${user.name || ""}`.trim(),
    });

    return response(res, 200, "Email verified successfully", {
      isEmailVerified: true,
    });
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {}
    return response(res, 500, err.message);
  } finally {
    client.release();
  }
};

exports.forgotPassword = async (req, res) => {
  const client = await pool.connect();
  try {
    const { email } = req.body;

    await client.query("BEGIN");

    const user = await getUserByEmail(
      client,
      email,
      "id, name, email, email_otp_last_sent_at",
      { forUpdate: true },
    );

    if (!user) {
      await client.query("COMMIT");
      return response(res, 200, "If the email exists, OTP has been sent");
    }

    if (!ensureCooldown(user.email_otp_last_sent_at)) {
      await client.query("COMMIT");
      return response(res, 429, "Please wait before requesting another OTP");
    }

    const otp = generateOtp();
    const otpHash = await hashPassword(otp);

    await client.query(
      `
        UPDATE users
        SET
          email_otp_code_hash = $1,
          email_otp_purpose = 'RESET_PASSWORD',
          email_otp_expires_at = NOW() + INTERVAL '${OTP_EXPIRY_HOURS} hour',
          email_otp_last_sent_at = NOW(),
          updated_at = NOW()
        WHERE id = $2
      `,
      [otpHash, user.id],
    );

    await client.query("COMMIT");

    await sendEmail({
      to: user.email,
      subject: "Reset your password",
      html: otpTemplate({ name: user.name, otp, purpose: "RESET_PASSWORD" }),
      text: `Your OTP is ${otp}. It is valid for 24 hours.`,
    });

    return response(res, 200, "OTP sent to your email");
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {}
    return response(res, 500, err.message);
  } finally {
    client.release();
  }
};

exports.resetPassword = async (req, res) => {
  const client = await pool.connect();
  try {
    const { email, otp, new_password } = req.body;

    await client.query("BEGIN");

    const user = await getUserByEmail(
      client,
      email,
      "id, email_otp_code_hash, email_otp_purpose, email_otp_expires_at",
      { forUpdate: true },
    );

    if (!user) {
      await client.query("ROLLBACK");
      return response(res, 400, "Invalid email or OTP");
    }

    if (
      !user.email_otp_code_hash ||
      user.email_otp_purpose !== "RESET_PASSWORD" ||
      !user.email_otp_expires_at ||
      new Date(user.email_otp_expires_at) < new Date()
    ) {
      await client.query("ROLLBACK");
      return response(res, 400, "OTP expired or invalid");
    }

    const match = await comparePassword(otp, user.email_otp_code_hash);
    if (!match) {
      await client.query("ROLLBACK");
      return response(res, 400, "Invalid email or OTP");
    }

    const passwordHash = await hashPassword(new_password);

    await client.query(
      `
        UPDATE users
        SET
          password_hash = $1,
          email_otp_code_hash = NULL,
          email_otp_purpose = NULL,
          email_otp_expires_at = NULL,
          email_otp_last_sent_at = NULL,
          updated_at = NOW()
        WHERE id = $2
      `,
      [passwordHash, user.id],
    );

    await client.query("COMMIT");
    return response(res, 200, "Password reset successful");
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {}
    return response(res, 500, err.message);
  } finally {
    client.release();
  }
};

exports.resendVerificationOtp = async (req, res) => {
  const client = await pool.connect();
  try {
    const { email } = req.body;

    await client.query("BEGIN");

    const user = await getUserByEmail(
      client,
      email,
      "id, name, email, is_email_verified, email_otp_last_sent_at",
      { forUpdate: true },
    );

    if (!user) {
      await client.query("COMMIT");
      return response(res, 200, "If the email exists, OTP has been sent");
    }

    if (user.is_email_verified) {
      await client.query("COMMIT");
      return response(res, 200, "Email already verified");
    }

    if (!ensureCooldown(user.email_otp_last_sent_at)) {
      await client.query("COMMIT");
      return response(res, 429, "Please wait before requesting another OTP");
    }

    const otp = generateOtp();
    const otpHash = await hashPassword(otp);

    await client.query(
      `
        UPDATE users
        SET
          email_otp_code_hash = $1,
          email_otp_purpose = 'VERIFY_EMAIL',
          email_otp_expires_at = NOW() + INTERVAL '${OTP_EXPIRY_HOURS} hour',
          email_otp_last_sent_at = NOW(),
          updated_at = NOW()
        WHERE id = $2
      `,
      [otpHash, user.id],
    );

    await client.query("COMMIT");

    await sendEmail({
      to: user.email,
      subject: "Verify your email",
      html: otpTemplate({ name: user.name, otp, purpose: "VERIFY_EMAIL" }),
      text: `Your OTP is ${otp}. It is valid for 24 hours.`,
    });

    return response(res, 200, "OTP sent to your email");
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {}
    return response(res, 500, err.message);
  } finally {
    client.release();
  }
};
