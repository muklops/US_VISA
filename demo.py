from US_VISA.logger import logging
from US_VISA.exception import USvisaException
import sys

try:
    a=4/0
except Exception as e:
    raise USvisaException(e, sys)

