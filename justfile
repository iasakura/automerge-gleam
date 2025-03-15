pre-commit:
    gleam test
    gleam format src test

check:
    gleam test
    gleam format src test --check

gen-test:
    cd test-gen \
    && pnpm run gen-test