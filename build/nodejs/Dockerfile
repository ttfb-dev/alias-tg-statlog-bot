FROM node:14

# создание директории приложения
WORKDIR /usr/src/app

COPY src ./src

COPY package.json       package.json
COPY yarn.lock          yarn.lock

RUN yarn install

EXPOSE 80

CMD [ "yarn", "start" ]
