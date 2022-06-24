# ETL_DATAGOKR

# 개요
- 최근에 집값이 떨어진다는 소문이 돌고있다. 그렇다면 내가 할 일을 슬슬 각을보면서 간을 보는 것.
- DATA.GO.KR에서 제공해주는 부동산 거래 정보를 사용하여 동향을 파악하자.

# DAG Deployment 절차
```bash
# 최초에 git clone
git clone https://github.com/dlgldgldgld/ETL_DATAGOKR.git\

# 이후부터는 pull로 update
git pull

# 아래 copy 명령어 수행.
cp -r ETL_DATAGOKR/dags/* airflow/dags
cp -r ETL_DATAGOKR/const airflow/dags
cp -r ETL_DATAGOKR/core airflow/dags
```

# RESULT
1. 매월 말일 Airflow를 통해 자동으로 사용자들에게 메일 전송.
![image](https://user-images.githubusercontent.com/18378009/175486494-76e85f67-2e5b-478c-b51b-7696f272519a.png)

2. 한눈에 볼수있도록 이전달과 이번달의 통게를 비교한 이미지 제공.
- 거래금액
![image](https://user-images.githubusercontent.com/18378009/175487496-13cb4304-391b-487d-9ec5-39e5ecf174a4.png)

- 거래량
![image](https://user-images.githubusercontent.com/18378009/175487576-e712618c-3b8b-4189-ab62-34ae25207ac5.png)
