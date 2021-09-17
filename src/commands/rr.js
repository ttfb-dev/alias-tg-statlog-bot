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

const rrQuery = `
select count(*) as users,
  count
from (
    select count(*) as count,
      user_id
    from (
        select user_id,
          toDate(created_at) as date
        from analytics
        where event = 'app.open'
          and user_id not in (100)
          and user_id not in (
            select user_id
            from analytics
            where event = 'app.open'
              and toDate(created_at) < '2021-08-30'
            group by user_id
          )
        group by date,
          user_id
        order by date desc
      )
    group by user_id
    order by count DESC
  )
group by count
`;

const rr = {
  get: async () => {
    return toText(await select());
  },
};

const select = async () => {
  try {
    const rrData = await clickhouse.query(rrQuery).toPromise();

    return {
      rrData,
    };
  } catch ({ message }) {
    console.error("rr select failed", message);
  }
};

const toText = (data) => {
  let result = "";

  result += "<b>Возвращаемость пользователей:</b>\n";
  result += "\n<b>Дней / пользователей</b>\n\n";

  data.rrData.forEach((row) => {
    result += `${row.count} / ${row.users}\n`;
  });

  return result;
};

export default rr;
