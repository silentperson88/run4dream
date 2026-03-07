const { body } = require('express-validator');

exports.createBankValidationRule = () => {
  return [body('bankName').isString().notEmpty().withMessage('Bank name is required')];
};
