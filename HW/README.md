# Homework Tasks
Ссылка на [HW1](https://docs.google.com/presentation/d/14uwhDjVXnT5LSGlpU8c0iceisX9e54DkfLcQwOeFEww)

Ссылка на [HW2](https://docs.google.com/presentation/d/1EjaJDNgK8ZX31ggJJalQ5zv6ri6rSco03ac7b-yeg3g)

Ссылка на [HW3](https://docs.google.com/presentation/d/1mVJ06oszyJFY8TQgfkFu-s51bVnA3rB7VRGeafkhprU/edit#slide=id.g2a5bcd20df6_0_116)

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

Скриншоты и юпитер-ноутбуки доступны [тут](./HW1/part1)

2. Написание map reduce на Python

Скриншоты и юпитер-ноутбуки доступны [тут](./HW1/part2)

## HW2

Скриншоты и юпитер-ноутбук доступны [тут](./HW2)

## HW3

Скриншоты и юпитер-ноутбук доступны [тут](./HW3)
