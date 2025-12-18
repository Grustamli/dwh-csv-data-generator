import csv
import dataclasses
from locale import currency
from logging import RootLogger
from statistics import quantiles

import factory


@dataclasses.dataclass
class Row:
    email: str
    uuid: str
    phone_number: str
    test_identifier: str
    event_name: str
    timestamp: str
    test_parameter1: str
    test_parameter2: str
    email_optin: bool
    test_attr: str
    age: int
    country: str
    whatsapp_optin: bool
    birthday: str
    currency: str
    quantity: int
    event_group_id: str
    unit_sale_price: float
    unit_price: float
    updated_at: str


class RowFactory(factory.Factory):
    class Meta:
        model = Row

    email = factory.Faker("email")
    uuid = factory.Faker("uuid4")
    phone_number = factory.Faker("phone_number")
    test_identifier = factory.Faker("uuid4")
    event_name = factory.Faker(
        "random_element",
        elements=[
            "confirmation_page_view",
            "event_insider",
            "cart_cleared",
            "s3_event",
            "test_event",
        ],
    )
    timestamp = factory.Faker("iso8601")
    test_parameter1 = factory.Faker("vin")
    test_parameter2 = factory.Faker("license_plate")
    email_optin = factory.Faker("boolean")
    test_attr = factory.Faker("word")
    age = factory.Faker("random_int", min=18, max=70)
    country = factory.Faker("country")
    whatsapp_optin = factory.Faker("boolean")
    birthday = factory.Faker("date")
    currency = factory.Faker("currency_code")
    quantity = factory.Faker("random_int", min=1, max=1000)
    event_group_id = factory.Faker("uuid4")
    unit_sale_price = factory.Faker(
        "pyfloat", right_digits=2, min_value=0, max_value=1000
    )
    unit_price = factory.Faker("pyfloat", right_digits=2, min_value=0, max_value=1000)
    updated_at = factory.Faker("iso8601")


def main():
    row_count = 10_000_000
    batch_size = 1_000_000
    with open(f"{row_count}_rows.csv", "a") as f:
        writer = csv.DictWriter(
            f, fieldnames=[fld.name for fld in dataclasses.fields(Row)]
        )
        writer.writeheader()

        for i in range(row_count // batch_size):
            rows = [RowFactory.build() for _ in range(batch_size)]
            writer.writerows(dataclasses.asdict(r) for r in rows)
            print(f"wrote batch {i + 1}")

    print("done writing rows")


if __name__ == "__main__":
    main()
