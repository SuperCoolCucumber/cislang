from cislang.scraper import WikiScraper
import sys

sys.setrecursionlimit(30000)

current = 'https://ru.wikipedia.org/wiki/%D0%9A%D0%B0%D1%82%D0%B5%D0%B3%D0%BE%D1%80%D0%B8%D1%8F:%D0%A0%D0%BE%D0%B4%D0%B8%D0%B2%D1%88%D0%B8%D0%B5%D1%81%D1%8F_%D0%B2_%D0%A2%D0%B1%D0%B8%D0%BB%D0%B8%D1%81%D0%B8'

links = WikiScraper().collect_links(current)
ids = WikiScraper().collect_ids(links)
arr = WikiScraper().build_array(ids)
WikiScraper().build_df(arr)
