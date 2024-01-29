from decimal import Decimal, ROUND_HALF_UP


def format_decimal(value, level):
    quantize_level = Decimal('1.{}'.format('0' * level))
    formatted_value = value.quantize(quantize_level, rounding=ROUND_HALF_UP)
    return float(formatted_value)
