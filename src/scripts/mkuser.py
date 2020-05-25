#!/usr/bin/env python
import click
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser


@click.command()
@click.argument("username")
@click.argument("email")
@click.option("--superuser", default=False, is_flag=True)
@click.password_option()
def make_user(username, email, superuser, password):
    user = PasswordUser(models.User())
    user.username = username
    user.email = email
    user.superuser = superuser
    user.password = password
    session = settings.Session()
    session.add(user)
    session.commit()
    session.close()


if __name__ == "__main__":
    make_user()
