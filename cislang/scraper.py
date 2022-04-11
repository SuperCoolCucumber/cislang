from typing import List
import requests
import bs4
from bs4 import BeautifulSoup
import re
import pandas as pd
from joblib import Parallel, delayed

class Scraper:
    def get_links(self, url: str) -> List[str]:
        raise NotImplemented
    def collect_links(self, current: str) -> List[bs4.element.Tag]:
        raise NotImplemented
    def collect_ids(self, links: List[bs4.element.Tag]) -> List[str]:
        raise NotImplemented
    def get_url(self, id_):
        raise NotImplemented
    def get_link(self, soup, lang):
        raise NotImplemented
    def make_wikidata_request(self, link):
        raise NotImplemented
    def build_array(self, ids):
        raise NotImplemented
    def build_df(self, arr):
        raise NotImplemented

    

class WikiScraper(Scraper):
    def get_links(self, url: str) -> List[str]:
        r = requests.get(url)
        doc = r.text
        soup = BeautifulSoup(doc, "html.parser")
        links = soup.find(id="mw-pages").find_all("a")
        return links
        
    def _check_if_contains(self, page, all_links):
        if len(all_links) != 0 and page in all_links[0]:
            return True
           
    def collect_links(self, current):
        all_links_collected = []
        i = 0
        while True: 
            try: 
                all_links = self.get_links(current)
            
            except AttributeError as e:
                all_links = []
            
            if self._check_if_contains('Следующая страница', all_links):
                next_page = all_links[0]
                all_links_collected.extend(all_links[1:])
                     
            elif self._check_if_contains('Предыдущая страница', all_links):
                next_page = all_links[1] 
                all_links_collected.extend(all_links[2:])
                
            else:
                break
            current = 'https://ru.wikipedia.org/'+BeautifulSoup.get(next_page, 'href')
            i+=1
            
        return all_links_collected



    def collect_ids(self, links: List[bs4.element.Tag]) -> List[str]:
        def inner_collect_ids(link):
            req = requests.get(
                "https://ru.wikipedia.org/" + BeautifulSoup.get(link, "href")
            )
            soup2 = BeautifulSoup(req.text, "html.parser")
            scr = str(soup2.find("script"))

            if re.findall('wgWikibaseItemId":\n"(\w+)"', scr):
                id_ = re.findall('wgWikibaseItemId":\n"(\w+)"', scr)
                return id_[0]
            elif re.findall('wgWikibaseItemId":"(\w+)"', scr):
                id_ = re.findall('wgWikibaseItemId":"(\w+)"', scr)
                return id_[0]   
        return list(
            filter(
                lambda x: True if x else False,
                Parallel(n_jobs=-1)(delayed(inner_collect_ids)(x) for x in (links))
                )
                )



    def get_url(id_):
     return f"https://www.wikidata.org/wiki/Special:EntityData/{id_}.json"


    def get_link(self, soup, lang):
        if soup.find_all(
            "span",
            attrs={
                "class": "wikibase-sitelinkview-link wikibase-sitelinkview-link-"
                + lang
                + "wiki"
            },
        ):
    
            return (soup.find_all(
                "span",
                attrs={
                    "class": "wikibase-sitelinkview-link wikibase-sitelinkview-link-"
                    + lang
                    + "wiki"
                },
            )[0]
            .find_all("a")[0]
            .get("href"))
        else: return ''
    


    def make_wikidata_request(self, link):
        link = "https://www.wikidata.org/wiki/" + link
        r = requests.get(link)
        doc = r.text
        soup = BeautifulSoup(doc, "html.parser")
        dic = {}
        for lang in ["ru", "ka", "en"]:
            dic[lang] = self.get_link(soup, lang)
        return dic



    def build_array(self, ids):
    
        def inner_build_array(id_):
            arr = []
            req = requests.get(self.get_url(id_)).json()
            langs = req["entities"][id_]["labels"]
            dic = self.make_wikidata_request(id_)

            if "ru" in langs.keys():
                arr.append(langs["ru"]["value"])
            else:
                arr.append("")
            arr.append(dic["ru"])
            if "en" in langs.keys():
                arr.append(langs["en"]["value"])
            else:
                arr.append("")
            arr.append(dic["en"])
            if "ka" in langs.keys():
                arr.append(langs["ka"]["value"])
            else:
                arr.append("")
            arr.append(dic["ka"])
            return arr
            
        return Parallel(n_jobs=-1)(delayed(inner_build_array)(x) for x in (ids))
        


    def build_df(self, arr):
        ka_df = pd.DataFrame(
            arr, columns=["ru_name", "ru_link", "en_name", "en_link", "ka_name", "ka_link"]
        )
        return ka_df.to_csv('/home/daria/cislang/ka_df.csv')