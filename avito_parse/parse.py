import asyncio
import re
from datetime import datetime, timedelta
from typing import Optional

import httpx
from aiogram import Bot, Dispatcher, types
from asyncpg import create_pool
from asyncpg.pool import Pool
from bs4 import BeautifulSoup, Tag

from avito_parse.db.advertisement import AdvertisementRepository
from avito_parse.db.user import UserRepository
from avito_parse.schema import Advertisement, User


def convert_data_marker_tag_to_advertisement(tag: Tag) -> Optional[Advertisement]:
    try:
        title_tag = tag.find_all('a', attrs={'data-marker': 'item-title'})[0]
    except Exception as e:
        raise e
    try:
        price_tag = tag.find_all('meta', attrs={'itemprop': 'price'})[0]
    except Exception as e:
        raise e

    try:
        address_tag = tag.find('div', attrs={'data-marker': 'item-address'})
    except Exception as e:
        raise e

    if address_tag is None:
        address_tag = tag.find('div', class_='item-address')
        if address_tag is None:
            try:
                address_tag = tag.select('span[class*="geo-address"]')[0]
            except IndexError:
                return None

    try:
        approximate_date_tag = tag.find_all('div', attrs={'data-marker': 'item-date'})[0]
    except Exception as e:
        raise e

    url = title_tag['href']
    title = str(title_tag.text).strip('\n')
    price = int(price_tag['content'])
    address = address_tag.text.strip('\n')
    approximate_date_string = approximate_date_tag.text.strip('\n')

    id_ = tag.get('data-item-id') or tag.get('id').replace('i', ' ')
    id_ = int(id_)

    ad = Advertisement(
        id=id_,
        url=url,
        title=title,
        price=price,
        address=address,
        approximate_date_string=approximate_date_string
    )
    return ad


word_to_month_mapping = {
    'января': 1,
    'февраля': 2,
    'марта': 3,
    'апреля': 4,
    'мая': 5,
    'июня': 6,
    'июля': 7,
    'августа': 8,
    'сентября': 9,
    'октября': 10,
    'ноября': 11,
    'декабря': 12,
}


def get_datetime_from_string(date_string: str) -> Optional[datetime]:
    """
    Преобразовать строку с датой в объект datetime.
    Возможны тhb варианта строки:
    1) 'Сегодня в 12:34'
    2) 'Вчера в 22:00'
    3) '15 ноября в 12:34'
    """
    date_string = date_string.lower().strip()
    time_string = re.findall(r'\d+:\d+', date_string)[0]
    time = datetime.strptime(time_string, '%H:%M')
    ad_date = datetime.now()
    # Строка первого формата
    if 'сегодня' in date_string:
        ad_date = ad_date.replace(
            hour=time.hour,
            minute=time.minute
        )

    elif 'вчера' in date_string:
        ad_date = ad_date.replace(
            hour=time.hour,
            minute=time.minute,
            day=(ad_date - timedelta(days=1)).day
        )
    # Строка второго формата
    else:
        try:
            day_number = int(re.findall(r'^\d{1,2}', date_string)[0])
            date_tokens = date_string.split()
            month_word = date_tokens[1]
        except Exception as e:
            raise e

        month_number = word_to_month_mapping.get(month_word)
        if not month_number:
            return None
        ad_date = ad_date.replace(
            month=month_number,
            day=day_number,
            hour=time.hour,
            minute=time.minute
        )

    return ad_date


async def enrich_ad_with_details(client: httpx.AsyncClient, ad: Advertisement) -> Advertisement:
    r = await client.get(url=ad.full_url)
    while r.status_code != 200:
        r = await client.get(url=ad.full_url)
        await asyncio.sleep(3)

    parser = BeautifulSoup(
        r.content.decode('utf-8'),
        'lxml'
    )
    try:
        date_tag = parser.find_all('div', class_='title-info-metadata-item-redesign')[0]
    except Exception as e:
        raise e
    date_string = date_tag.text.strip('\n')
    dt = get_datetime_from_string(date_string)
    ad.date = dt

    description_text_tag = parser.find('div', class_='item-description-text')
    description_html_tag = parser.find('div', class_='item-description-html')
    description_tag = description_text_tag or description_html_tag
    if description_tag is None:
        raise IndexError
    description = description_tag.text.strip()
    ad.description = description

    return ad


class Handlers:
    def __init__(self, user_repo: UserRepository):
        self.user_repo = user_repo

    async def start_handler(self, event: types.Message):
        user = await self.user_repo.get_user(id_=event.from_user.id)
        if not user:
            await self.user_repo.create_user(
                user=User(
                    id=event.from_user.id,
                    username=event.from_user.username,
                    first_name=event.from_user.first_name,
                    last_name=event.from_user.last_name,
                    chat_id=event.chat.id
                )
            )
        await event.answer(
            f"Hello, {event.from_user.get_mention(as_html=True)} 👋!",
            parse_mode=types.ParseMode.HTML,
        )


async def start_telegram_bot(bot: Bot, handlers: Handlers):
    print('start server')
    try:
        disp = Dispatcher(bot=bot)
        disp.register_message_handler(handlers.start_handler, commands={"start", "restart"})
        await disp.start_polling()
    finally:
        await bot.close()


def generate_message(ad: Advertisement) -> str:
    return f'<b>{ad.title}</b>\n' \
           f'Цена: {ad.formatted_price} р.\n' \
           f'Дата/время размещения: {ad.date}\n' \
           f'Адрес: {ad.address}\n' \
           f'<a href="{ad.full_url}">Ссылка на объявление</a>'


async def main():
    pool = await create_pool(
        host='localhost',
        port=5432,
        database='cs',
        user='cs_user',
        password='123456',
    )
    user_repo = UserRepository(pool=pool)
    ad_repo = AdvertisementRepository(pool=pool)

    bot_token = '1450384774:AAGhSBmAO81nN2TzjlRVIpUiutcDRWOJISc'
    bot = Bot(token=bot_token)
    handlers = Handlers(user_repo=user_repo)
    asyncio.create_task(start_telegram_bot(bot=bot, handlers=handlers))

    while True:
        print(datetime.now())
        users = await user_repo.get_users()
        async with httpx.AsyncClient() as client:
            client.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.183 Safari/537.36'
            r = await client.get(url='https://www.avito.ru/domodedovo/kvartiry/prodam-ASgBAgICAUSSA8YQ?cd=1&f=ASgBAQECAUSSA8YQAkDkBzT8Uf5R~FHKCCSEWYJZAUXiBxd7ImZyb20iOjUxODcsInRvIjpudWxsfQ&proprofile=1&s=104')
            parser = BeautifulSoup(r.content.decode('utf-8'), 'lxml')
            advertisement_div_tags = parser.find_all('div', attrs={'data-marker': 'item'})
            ads = []
            for ad_tag in advertisement_div_tags:
                ad = convert_data_marker_tag_to_advertisement(ad_tag)

                ad = await enrich_ad_with_details(
                    client=client,
                    ad=ad,
                )
                db_ad = await ad_repo.get_ad(id_=ad.id)
                if not db_ad:
                    await ad_repo.create_ad(ad=ad)
                    for user in users:
                        await bot.send_message(
                            chat_id=user.chat_id,
                            parse_mode='HTML',
                            text=generate_message(ad=ad)
                        )
                    ads.append(ad)
                    print(ad)
                await asyncio.sleep(0.5)
            print("Всего объявлений: ", len(ads), ads)

        await asyncio.sleep(120)


if __name__ == '__main__':
    asyncio.run(main())
