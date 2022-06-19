## Book DB Creator
출판사 정보로 도서 데이터베이스를 구축하는 프로그램입니다.

### 0. 프로젝트 구조

### 1. config 생성하기
서버 실행을 위해 필요한 환경변수를 정의합니다
``` python
import os
from dotenv import load_dotenv

from pathlib import Path

RUN_MODE = 'prod'   # 수행모드(test/prod)
test_pub_size = 2   # 검색할 출판사 갯수 지정(RUN_MODE가 test일 경우만 적용)

load_dotenv()

api_request_url = "naver_api_request_url"
API_CLIENT_ID = os.environ.get('NAVER_API_CLIENT_ID')
API_CLIENT_SECRET = os.environ.get('NAVER_API_CLIENT_SECRET')

MY_SQL_DATABASE_URI = 'mysql_connection_url'

CSV_PATH = Path(__file__).resolve().parent
CSV_FILE_EXT = '.csv'
```

### 2. 도서 정보 csv로 저장하기
**[GET] /search**
> query parameter로 전달한 출판사의 도서목록을 저장한 csv 파일을 data/search/ 에 생성한다
``` commandline
/search?publisher={출판사명}
```
``` json
{ "status": "success", "가나문화콘텐츠": 50 }
```
``` json
{
  "status": "fail",
  "warning": "Pass publisher string as a query parameter.",
  "publisher_list": [ ]
}
```
``` json
{ "status": "fail", "warning": "가나문화콘텐츠.csv already exists."}
```
 

### 3. 도서 세부 정보(category, detail) 데이터베이스에 저장하기
**[GET] /save**
> query parameter로 전달한 출판사의 도서목록을 저장한 csv 파일을 읽어
> 세부 정보를 담은 새로운 csv 파일을 data/save/에 생성하고 데이터베이스에 저장한다.
``` commandline
/save?publisher={출판사명}
```
``` json
{ "status": "success", "가나문화콘텐츠": 50 }
```
``` json
{
  "status": "fail",
  "warning": "Pass publisher string as a query parameter.",
  "publisher_list": [ ]
}
```

**[GET] /save/all**
> data/search/에 존재하는 모든 출판사 도서목록 csv 파일에 대한 세부정보를 크롤링하여
> 새로운 csv 파일을 data/save/에 생성하고 데이터베이스에 저장한다.
``` commandline
/save/all
```
``` json
{
 "status": "success", 
 "가나문화콘텐츠": 50,
 "시공사": 8000,
 "문학동네": 5700
}
```