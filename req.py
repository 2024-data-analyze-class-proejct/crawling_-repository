import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import pandas as pd


start_date = datetime.strptime("2023-5-22", "%Y-%m-%d")
end_date = datetime.strptime("2024-11-20", "%Y-%m-%d")

while start_date <= end_date:
    CURRENT_DATE = start_date.strftime("%Y-%m-%d")

    con = requests.get("https://www.kobis.or.kr/kobis/business/stat/boxs/findDailyBoxOfficeList.do")
    csrfGet = BeautifulSoup(con.text, "html.parser")
    csrf = csrfGet.find("form", {"id": "searchForm"}).find("input", {"name": "CSRFToken"})["value"]

    post = requests.post("https://www.kobis.or.kr/kobis/business/stat/boxs/findDailyBoxOfficeList.do", {
        "CSRFToken": csrf,
        "loadEnd": "0",
        "searchType": "search",
        "sSearchFrom": CURRENT_DATE,
        "sSearchTo": CURRENT_DATE,
    })

    rootDic = {}

    movieDatas = BeautifulSoup(post.text, "html.parser")
    mapToStr = lambda x: x.text.strip().replace("\t", "").replace("\r", "").replace("\n", "").replace("  ", "")

    def filterCrawl(data):
        if not "onclick" in data.attrs:
            return False
        if not data['onclick'].startswith("mstView"):
            return False
        return True
        
    targetLoop = [*filter(filterCrawl, movieDatas.find_all("a"))]

    print(f"Crawling data for {len(targetLoop)} elements")

    for data in targetLoop:
        dic = {}
        par = data.parent.parent.parent.find_all('td')

        # for p in par:
        #     print(p.text.strip())
        
        for key, value in zip(
            ["순위", "영화명", "개봉일", "매출액", "매출액점유율", "매출액증감(전일대비)", "누적매출액", "관객수", "관객수증감(전일대비)", "누적관객수", "스크린수", "상영횟수"],
            map(mapToStr, par)
        ): dic[key] = value

        movieCode = data["onclick"].replace("mstView(", "").replace(");", "").split(",")[1].replace("'", "").replace("return false;", "")
        
        res = requests.post('https://www.kobis.or.kr/kobis/business/mast/mvie/searchMovieDtl.do', {
            "code": movieCode,
            "sType": "",
            "titleYN": "Y",
            "etcParam": "",
            "isOuterReq": "false",
            "CSRFToken": csrf
        })
        info = BeautifulSoup(res.text, "html.parser")
        for some in info.find_all("dl"):
            hasChild = some.find("dd").findChildren()
            if hasChild and hasChild[0].name == "a":
                zipped = zip(map(mapToStr, some.find_all("dt")), map(lambda d: mapToStr(d.findChildren()[0]), some.find_all("dd")))
            else:
                zipped = zip(map(mapToStr, some.find_all("dt")), map(mapToStr, some.find_all("dd")))

            for key, value in zipped: dic[key] = value

        actorJson = requests.post("https://www.kobis.or.kr/kobis/business/mast/mvie/searchMovActorLists.do", {
            "movieCd": movieCode,
            "CSRFToken": csrf
        }, headers={
            "accept": "application/json, text/javascript, */*; q=0.01",
        }).json()

        actors = []
        for actor in actorJson:
            actors.append(actor["peopleNm"])
        dic["배우"] = actors[0] if actors else "없음"

        staffJson = requests.post("https://www.kobis.or.kr/kobis/business/mast/mvie/searchMovStaffLists.do", {
            "movieCd": movieCode,
            "mgmtMore": "N",
            "CSRFToken": csrf
        }, headers={
            "accept": "application/json, text/javascript, */*; q=0.01",
        }).json()

        staffs = []
        for staff in staffJson:
            staffs.append(staff["peopleNm"])
        dic["감독"] = staffs[0] if staffs else "없음"
        dic["검색날짜"] = CURRENT_DATE
        rootDic[movieCode] = dic
        print("Crawled data for", dic["영화명"])

    print("day ends... saving!")
    path = '/Users/joshmoon827/crowling/fullfinalrealresult.csv'

    df = pd.DataFrame(rootDic.values())
    try:
        existing_df = pd.read_csv(path)
        df = pd.concat([existing_df, df], ignore_index=True)
    except FileNotFoundError:
        pass
    df.to_csv(path, index=False, encoding='utf-8')
    print(df.head())
    print(f"CSV 파일이 {path}에 저장되었습니다.")

    start_date += timedelta(days=1)

