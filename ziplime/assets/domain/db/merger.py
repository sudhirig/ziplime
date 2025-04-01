from sqlalchemy.orm import Mapped

from ziplime.db.annotated_types import StringIndexed, DateIndexed, IntegerIndexed, StringPK
from ziplime.db.base_model import BaseModel


class Merger(BaseModel):
    __tablename__ = "mergers"

    index: Mapped[StringPK]
    sid: Mapped[IntegerIndexed]

    effective_date: Mapped[DateIndexed]
    ratio: Mapped[float]
