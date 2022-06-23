# ETL_DATAGOKR

## 개요
- 최근에 집값이 떨어진다는 소문이 돌고있다. 그렇다면 내가 할 일을 슬슬 각을보면서 간을 보는 것.
- DATA.GO.KR에서 제공해주는 부동산 거래 정보를 사용하여 동향을 파악하자.

## 구현 하고싶은 기능
- 최근 3개월간의 시군구별로 부동산 거래 평균 추출.
- 작은 평수의 매물이 팔려 있었을 수도 있으므로 평당 평균 거래 내역 추출.
- 시군구내에서도 위치에 따라 집값의 차이가 있을 수 있으므로 도로명 주소까지 사용해볼 것.
  - ex ) 재송동 - 위쪽, 아래쪽 집값의 차이는 꽤큼.
- ML을 통해 insight 예측 가능한 Model이 있는지 조사 후 적용 유무 결정.

# [ Temporary memo ]
## DAG Deployment
```bash
git clone https://github.com/dlgldgldgld/ETL_DATAGOKR.git\
cp -r ETL_DATAGOKR/dags/* airflow/dags
cp -r ETL_DATAGOKR/const airflow/dags
cp -r ETL_DATAGOKR/core airflow/dags
cp -r ETL_DATAGOKR/statistics airflow/dags
cp -r ETL_DATAGOKR/font airflow/dags
```

## 부산광역시 4-5월 동향
![image](https://user-images.githubusercontent.com/18378009/171413443-43f93599-0600-4997-aaf0-c66ea995fe66.png)
- 조큼 수상하다..

## 서울 4-5월 동향
![image](https://user-images.githubusercontent.com/18378009/171634523-66f8c2b6-a837-4da9-b221-81fe4ce4a3a1.png)
- ??!
