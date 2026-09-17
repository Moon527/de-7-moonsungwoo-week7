# Week 7 Data Engineering

- Name: moonsungwoo
- 이름: 문성우
- Cohort: 7

This repository contains the week 7 assignment project.

## 실습 환경

Windows의 Ubuntu WSL2와 Docker Desktop에서 Airflow와 Spark 실습을 진행한다.

- Airflow: 2.10.5, CeleryExecutor, PostgreSQL 13, Redis 7.2.
- Custom image: moonsungwoo-airflow:2.10.5, Python 3.8, JDK 17.
- Spark: 3.5.7, PySpark 3.5.7.
- AWS: ap-northeast-2, private S3 bucket de-7-moonsungwoo.
- Local UI: Airflow localhost:8080, Spark master localhost:8081.

## 회고

작업 성공 여부를 로그와 실제 출력 데이터로 확인하고 문항별 실행 증빙을 보관한다.

## 데이터와 보안

원본 CSV, 실행 로그, 출력 데이터 및 자격증명은 Git에서 제외한다. AWS 작업은 기존 week7 프로필을 사용하며 키를 소스나 이미지에 넣지 않는다. Airflow worker에만 자격증명 파일을 읽기 전용으로 연결한다. 이 구성은 로컬 수업 실습용이며 운영 배포용이 아니다.

Q1/Q2 제출 코드와 Q2 출력, Q3~Q10 증빙은 최종 제출 폴더의 각 Qn 디렉토리에 있다. 이 저장소 루트가 제출 폴더의 project/에 대응한다.
