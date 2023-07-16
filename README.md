# DSIT Big Data Management Project

This is the final project for the Big Data Management course of the Data Science and Information Technologies MSc
program at the University of Athens.

## Project Description

The project description can be found [here](Project.pdf)

## Dataset Overview

#### movies.csv

| movie_id | name            | description                     | release_year | duration | production_cost | revenue | popularity |
| -------- | --------------- | ------------------------------- | ------------ | -------- | --------------- | ------- | ---------- |
| 1        | The Great Gatsby| A tale of wealth and decadence   | 2013         | 143      | 105000000       | 353600000| 7.3        |

#### ratings.csv

| user_id | movie_id | rating | timestamp           |
| ------- | -------- | ------ | ------------------- |
| 1       | 1        | 4.5    | 2023-06-29 10:15:00 |

#### movie_genres.csv

| movie_id | genre  |
| -------- | ------ |
| 1        | Drama  |

## Solution

### Part 1

#### Task 1

```bash
wget https://www.dropbox.com/s/c10t67glk60wpha/datasets2023.tar.gz?dl=0
mv datasets2023.tar.gz?dl=0 datasets.tar.gz
tar -xzf datasets.tar.gz
hadoop fs -mkdir files
hadoop fs -copyFromLocal * files
hadoop fs -ls
```

#### Task 2

- [Q1](solution/part1/task2/q1.py)
- [Q2](solution/part1/task2/q2.py)
- [Q3](solution/part1/task2/q3.py)
- [Q4](solution/part1/task2/q4.py)
- [Q5](solution/part1/task2/q5.py)