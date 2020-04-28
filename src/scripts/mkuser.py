#!/usr/bin/env python
import click
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser


@click.command()
@click.argument("username")
@click.argument("email")
def make_user(username, email):
    user = PasswordUser(models.User())
    user.username = username
    user.email = email
    user.superuser = True
    user.password = click.prompt("Password")
    session = settings.Session()
    session.add(user)
    session.commit()
    session.close()


if __name__ == "__main__":
    make_user()
