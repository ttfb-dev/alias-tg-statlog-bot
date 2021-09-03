import { ClickHouse } from "clickhouse";

const clickhouse = new ClickHouse({
  url: process.env.CLICKHOUSE_HOST,
  port: process.env.CLICKHOUSE_PORT,
  debug: false,
  basicAuth: {
    username: process.env.CLICKHOUSE_USER,
    password: process.env.CLICKHOUSE_PASSWORD,
  },
  isUseGzip: false,
  format: "json",
  raw: false,
  config: {
    session_timeout: 60,
    output_format_json_quote_64bit_integers: 0,
    enable_http_compression: 0,
    database: process.env.CLICKHOUSE_DB,
  },
});

const dailyGamesStartQuery = `
  SELECT 
    toDate(created_at) as date, 
    count(*) as cnt 
  FROM analytics
  WHERE event = 'game.start' 
  GROUP BY date 
  ORDER BY date DESC 
  LIMIT 7
`;

const dailyGamesFinishedWithWinnerQuery = `
  SELECT 
    toDate(created_at) as date, 
    count(*) as cnt
  FROM analytics
  WHERE 
    event = 'game.finish'
    AND JSONExtractString(payload, 'reason') = 'has_winner'
  GROUP BY date 
  ORDER BY date DESC 
  LIMIT 7
`;

const dailyOnboardingFinishedQuery = `
  SELECT 
    toDate(created_at) as date, 
    count(*) as cnt
  FROM analytics
  WHERE 
    event = 'profile.finish_onboarding'
  GROUP BY date 
  ORDER BY date DESC 
  LIMIT 7
`;

const dailyRoomJoinedQuery = `
  SELECT 
    toDate(created_at) as date,
    JSONExtractString(payload, 'join_method') as method,
    count(*) as cnt
  FROM analytics
  WHERE 
    event = 'room.join'
  GROUP BY date, method
  ORDER BY date DESC 
  LIMIT 21
`;

const statistic = {
  get: async () => {
    return toText(await select());
  },
};

const select = async () => {
  try {
    const dailyGamesStart = await clickhouse
      .query(dailyGamesStartQuery)
      .toPromise();
    const dailyGamesFinishedWithWinner = await clickhouse
      .query(dailyGamesFinishedWithWinnerQuery)
      .toPromise();
    const dailyOnboardingFinished = await clickhouse
      .query(dailyOnboardingFinishedQuery)
      .toPromise();
    const dailyRoomJoined = await clickhouse
      .query(dailyRoomJoinedQuery)
      .toPromise();
    return {
      dailyGamesStart,
      dailyGamesFinishedWithWinner,
      dailyOnboardingFinished,
      dailyRoomJoined,
    };
  } catch ({ message }) {
    console.error("analytics select failed", message);
  }
};

const toText = (data) => {
  let result = "";

  result += "<b>Ежедневная статистика:</b>\n";

  result += "\n<b>Онбордингов закончено</b>\n";

  data.dailyOnboardingFinished.forEach((row) => {
    result += `${row.date}: ${row.cnt}\n`;
  });

  result += "\n<b>Присоединилось к комнатам</b>\n";

  data.dailyOnboardingFinished.forEach((row) => {
    console.log(row);
    result += `${row.date} (${row.method}): ${row.cnt}\n`;
  });

  result += "\n<b>Игр с победителем</b>\n";

  data.dailyGamesFinishedWithWinner.forEach((row) => {
    result += `${row.date}: ${row.cnt}\n`;
  });

  result += "\n<b>Игр начато</b>\n";

  data.dailyGamesStart.forEach((row) => {
    result += `${row.date}: ${row.cnt}\n`;
  });

  return result;
};

export default statistic;
