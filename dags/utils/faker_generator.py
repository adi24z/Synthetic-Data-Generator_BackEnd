from faker import Faker
import random


fake = Faker()


def generate_fake_value(column):

    column = column.lower()

    if "name" in column:
        return fake.name()

    elif "email" in column:
        return fake.email()

    elif "phone" in column:
        return fake.phone_number()

    elif "city" in column:
        return fake.city()

    elif "address" in column:
        return fake.address()

    elif "date" in column:
        return str(fake.date())

    elif "id" in column:
        return random.randint(1000, 9999)

    else:
        return fake.word()