pre-commit:
    gleam test
    gleam format src test

check:
    gleam test
    gleam format src test --check