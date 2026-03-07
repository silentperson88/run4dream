const APP_NAME = process.env.APP_NAME || "Paper Trading";
const APP_URL = process.env.APP_URL || "";

function baseTemplate({ title, content }) {
  return `
  <div style="font-family: 'Segoe UI', Arial, sans-serif; background:#f5f7fb; padding:24px;">
    <div style="max-width:600px;margin:0 auto;background:#ffffff;border-radius:12px;overflow:hidden;border:1px solid #eef1f6;">
      <div style="background:#0f172a;color:#fff;padding:18px 24px;font-size:18px;font-weight:600;">
        ${APP_NAME}
      </div>
      <div style="padding:24px;color:#0f172a;line-height:1.6;">
        <h2 style="margin:0 0 12px;font-size:20px;">${title}</h2>
        ${content}
        ${APP_URL ? `<p style="margin-top:24px;"><a style="color:#2563eb;text-decoration:none;" href="${APP_URL}">Open ${APP_NAME}</a></p>` : ""}
      </div>
      <div style="padding:16px 24px;color:#6b7280;font-size:12px;background:#f9fafb;">
        If you didn't request this, you can ignore this email.
      </div>
    </div>
  </div>
  `;
}

function otpTemplate({ name, otp, purpose }) {
  const title =
    purpose === "RESET_PASSWORD" ? "Reset Your Password" : "Verify Your Email";
  const content = `
    <p>Hi ${name || "there"},</p>
    <p>Your OTP is:</p>
    <div style="font-size:28px;font-weight:700;letter-spacing:4px;background:#f1f5f9;padding:12px 16px;border-radius:8px;display:inline-block;">
      ${otp}
    </div>
    <p style="margin-top:16px;">This OTP is valid for 24 hours.</p>
  `;
  return baseTemplate({ title, content });
}

function welcomeTemplate({ name }) {
  const content = `
    <p>Hi ${name || "there"},</p>
    <p>Welcome to ${APP_NAME}! You are all set to start trading with your paper portfolio.</p>
    <div style="margin-top:16px;padding:12px;border-left:4px solid #22c55e;background:#f0fdf4;">
      Tip: Create a portfolio and start tracking performance right away.
    </div>
  `;
  return baseTemplate({ title: "Welcome!", content });
}

module.exports = { otpTemplate, welcomeTemplate };
