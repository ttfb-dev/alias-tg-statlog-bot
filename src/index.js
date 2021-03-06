import dotenv from "dotenv";
import TelegramBot from "node-telegram-bot-api";

import statistic from "./commands/statistic.js";
import rr from "./commands/rr.js";

dotenv.config();

const token = process.env("TG_STATLOG_BOT_API");

// Create a bot that uses 'polling' to fetch new updates
const bot = new TelegramBot(token, { polling: true });

// Matches "/echo [whatever]"
bot.onText(/\/echo (.+)/, (msg, match) => {
  // 'msg' is the received Message from Telegram
  // 'match' is the result of executing the regexp above on the text content
  // of the message

  const chatId = msg.chat.id;
  const resp = match[1]; // the captured "whatever"

  // send back the matched "whatever" to the chat
  bot.sendMessage(chatId, resp);
});

// Listen for any kind of message. There are different kinds of
// messages.
bot.on("message", async (msg) => {
  const chatId = msg.chat.id;

  const responseMessage = await getResponse(msg.text);

  // send a message to the chat acknowledging receipt of their message
  bot.sendMessage(chatId, responseMessage, { parse_mode: "HTML" });
});

const getResponse = async (message) => {
  switch (message) {
    case "/start":
      return `Hi!`;
    case "/statistic":
      return await statistic.get();
    case "/rr":
      return await rr.get();
    default:
      return `Unknown command: <code>${message}</code>`;
  }
};
