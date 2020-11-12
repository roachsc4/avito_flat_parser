from typing import Optional, List

from asyncpg.pool import Pool

from avito_parse.schema import User


class UserRepository:
    def __init__(self, pool: Pool):
        self.pool = pool

    async def get_user(self, id_: int) -> Optional[User]:
        row = await self.pool.fetchrow(
            """
            SELECT id, username, first_name, last_name, chat_id
            FROM auth.user
            WHERE id = $1
            """,
            id_
        )
        if not row:
            return None

        return User(
            id=row['id'],
            username=row['username'],
            first_name=row['first_name'],
            last_name=row['last_name'],
            chat_id=row['chat_id']
        )

    async def get_users(self) -> List[User]:
        rows = await self.pool.fetch(
            """
            SELECT id, username, first_name, last_name, chat_id
            FROM auth.user
            """
        )

        return [
            User(
                id=row['id'],
                username=row['username'],
                first_name=row['first_name'],
                last_name=row['last_name'],
                chat_id=row['chat_id']
            )
            for row in rows
        ]

    async def create_user(self, user: User):
        await self.pool.execute(
            """
            INSERT INTO auth.user(id, username, first_name, last_name, chat_id)
            VALUES ($1, $2, $3, $4, $5)
            """,
            user.id, user.username, user.first_name, user.last_name, user.chat_id
        )
