import { StepExecutionContext, StepExecutionResult } from '../execution.types.js';
import nodemailer from 'nodemailer';
import config from '../../../config/env.js';

type SendEmailConfig = {
  provider: 'sendgrid' | 'smtp';
  from: string;
  to: string[];
  cc?: string[];
  bcc?: string[];
  subject: string;
  text?: string;
  html?: string;
  replyTo?: string;
};

export async function executeEmailAction(config: SendEmailConfig, _context: StepExecutionContext): Promise<StepExecutionResult> {
  if (!config.provider || !config.from || !config.to?.length || !config.subject) {
    return { success: false, error: 'send_email requires provider, from, to, and subject' };
  }

  if (!config.text && !config.html) {
    return { success: false, error: 'send_email requires text or html content' };
  }

  if (config.provider === 'smtp') {
    return sendViaSmtp(config);
  }

  if (config.provider === 'sendgrid') {
    return sendViaSendgrid(config);
  }

  return { success: false, error: `Unsupported email provider: ${config.provider}` };
}

let smtpTransporter: nodemailer.Transporter | null = null;

async function sendViaSmtp(email: SendEmailConfig): Promise<StepExecutionResult> {
  if (!config.smtpHost || !config.smtpUser || !config.smtpPass) {
    return { success: false, error: 'SMTP_HOST, SMTP_USER and SMTP_PASS must be configured for smtp provider' };
  }

  if (!smtpTransporter) {
    smtpTransporter = nodemailer.createTransport({
      host: config.smtpHost,
      port: config.smtpPort,
      secure: config.smtpSecure,
      auth: {
        user: config.smtpUser,
        pass: config.smtpPass,
      },
    });
  }

  try {
    const result = await smtpTransporter.sendMail({
      from: email.from || config.smtpDefaultFrom,
      to: email.to,
      cc: email.cc,
      bcc: email.bcc,
      subject: email.subject,
      text: email.text,
      html: email.html,
      replyTo: email.replyTo,
    });

    return {
      success: true,
      output: {
        provider: 'smtp',
        messageId: result.messageId,
        accepted: result.accepted,
        rejected: result.rejected,
      },
    };
  } catch (error: any) {
    return {
      success: false,
      error: error?.message || 'SMTP send failed',
    };
  }
}

async function sendViaSendgrid(email: SendEmailConfig): Promise<StepExecutionResult> {
  if (!config.sendgridApiKey) {
    return { success: false, error: 'SENDGRID_API_KEY must be configured for sendgrid provider' };
  }

  const payload = {
    personalizations: [
      {
        to: email.to.map((address) => ({ email: address })),
        cc: email.cc?.map((address) => ({ email: address })),
        bcc: email.bcc?.map((address) => ({ email: address })),
      },
    ],
    from: { email: email.from },
    subject: email.subject,
    content: [
      ...(email.text ? [{ type: 'text/plain', value: email.text }] : []),
      ...(email.html ? [{ type: 'text/html', value: email.html }] : []),
    ],
    reply_to: email.replyTo ? { email: email.replyTo } : undefined,
  };

  try {
    const response = await fetch('https://api.sendgrid.com/v3/mail/send', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${config.sendgridApiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    });

    if (response.status < 200 || response.status >= 300) {
      const body = await response.text().catch(() => '');
      return {
        success: false,
        error: `SendGrid request failed with status ${response.status}: ${body}`,
      };
    }

    return {
      success: true,
      output: {
        provider: 'sendgrid',
        statusCode: response.status,
      },
    };
  } catch (error: any) {
    return {
      success: false,
      error: error?.message || 'SendGrid send failed',
    };
  }
}
