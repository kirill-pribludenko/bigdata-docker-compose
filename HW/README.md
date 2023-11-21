# Homework Tasks
Ссылка на [HW1](https://docs.google.com/presentation/d/14uwhDjVXnT5LSGlpU8c0iceisX9e54DkfLcQwOeFEww)
Ссылки на 2 и 3 домашнии работы появятся по мере прохождения курса

**REMARK** 

Чтобы избежать ошибки в Win системах подобной этой

`
/opt/hadoop/etc/hadoop/hadoop-env.sh: line 127: $'\r': command not found
`

Нужно после запуска докер контейнеров в первой ячейке ввести следующую команду:

`
!sed -i 's/\r$//' /opt/hadoop/etc/hadoop/hadoop-env.sh
`
# Homework Solutions:
## HW1
1. Развертывание локального кластера

Скриншоты и юпитер-ноутбуки доступны [тут](./HW/HW1/part1)

2. Написание map reduce на Python

Скриншоты и юпитер-ноутбуки доступны [тут](./HW/HW1/part2)

