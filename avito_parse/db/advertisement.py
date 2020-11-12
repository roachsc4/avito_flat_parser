import logging
from datetime import datetime
from typing import Optional

import asyncpg
from asyncpg.pool import Pool

from avito_parse.schema import Advertisement

logger = logging.getLogger('db_logger')


class AdvertisementRepository:
    def __init__(self, pool: Pool):
        self.pool = pool

    async def create_ad(self, ad: Advertisement):
        try:
            result = await self.pool.execute(
                """
                INSERT INTO advertisement.ad(id, url, title, price, address, approximate_date_string, date, description)
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8
                )
                ON CONFLICT (id)
                DO UPDATE SET price=$4, date=$7
                """,
                ad.id,
                ad.url,
                ad.title,
                ad.price,
                ad.address,
                ad.approximate_date_string,
                ad.date,
                ad.description
            )
            print(result)
        except asyncpg.UniqueViolationError as e:
            logger.warning(e)

    async def get_ad(self, id_: int) -> Optional[Advertisement]:
        row = await self.pool.fetchrow(
            """
            SELECT  id,
                    url,
                    title,
                    price,
                    address,
                    approximate_date_string,
                    date,
                    description
            FROM advertisement.ad
            WHERE id=$1
            """,
            id_
        )
        if not row:
            return None

        return Advertisement(
            id=id_,
            url=row['url'],
            title=row['title'],
            price=row['price'],
            address=row['address'],
            approximate_date_string=row['approximate_date_string'],
            date=row['date'],
            description=row['description'],
        )
