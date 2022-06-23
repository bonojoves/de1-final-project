# imports

import pandas as pd
from datetime import datetime
import os

## Weather
from meteostat import Point, Daily

## NIKL prices
from fastquant import get_pse_data

## feed parsing news - economic news, etc
import feedparser

## import spacy for transformation
import spacy
import ast