# coding=utf-8
from email.mime.text import MIMEText
import smtplib
from bs4 import BeautifulSoup
import mechanize
import hashlib

# Проверяемые учетные записи
ACCOUNTS = {
    '2003049821': {
        'title': 'Smart BP AZ',
        'password': 'FB686',
        'source_hash': '3750c68912ce905ebf724df2434dda5e'
    },

    '2003062681': {
        'title': 'Odlar Yurdu',
        'password': '231F6',
        'source_hash': '3750c68912ce905ebf724df2434dda5e'
    },

    '2003049541': {
        'title': 'Smart Financial Solutions',
        'password': 'F6A9B',
        'source_hash': '3750c68912ce905ebf724df2434dda5e'
    }
}

# Список системных пользователей, которые получают уведомления о проблемах с работой скрипта
STAFF_RECIPIENTS = (
    'dibrovsd@smart-bp.ru',
    'd.ironclad@gmail.com'
)

# Список пользователей, которые должны получать уведомления о проверках
WARNING_RECIPIENTS = (
    'pavel.kuzko@gmail.com',
    'e.mustafayevsfs@hotmail.com',
    'suleymanov@me.com'
)

# Настройка для электронной почты
SERVER_EMAIL = 'no-reply@smart-bp.ru'
EMAIL_HOST = 'smtp.yandex.ru'
EMAIL_HOST_USER = SERVER_EMAIL
EMAIL_HOST_PASSWORD = 'fsjhdfge5vdfgd'
EMAIL_USE_TLS = True
EMAIL_PORT = 587

LOGIN_URL = 'http://www.yoxlama.gov.az/login.aspx?ReturnUrl=%2fDefault.aspx'
LOGOUT_URL = 'http://www.yoxlama.gov.az/Logout.aspx'

# Инициализация браузера страниц
br = mechanize.Browser()
br.set_handle_equiv(True)
br.set_handle_redirect(True)
br.set_handle_referer(True)
br.set_handle_robots(False)
br.set_handle_redirect(mechanize.HTTPRedirectHandler)
br.set_handle_refresh(mechanize.HTTPRefreshProcessor(), max_time=1)


def yoxlamalar_login(_br, _login, _password):
    u"""
    Логин на сайт
    :param _br: инстанс mechanize.Browser
    :param _login: логин пользователя на сайте
    :param _password: пароль пользователя на сайте
    :return: ответ на отправку формы входа
    """
    _br.open(LOGIN_URL)
    _br.select_form(nr=0)
    _br['TabContainer1$tabLogin$Login1$UserName'] = _login
    _br['TabContainer1$tabLogin$Login1$Password'] = _password
    _br.submit(nr=0)
    return _br.response()


def send_mail(recipients, subject, message):
    u"""
    Отправка почты
    :param recipients:
    :param subject:
    :param message:
    :return:
    """
    msg = MIMEText(message, 'html')
    msg['Subject'] = subject
    msg['From'] = SERVER_EMAIL
    msg['To'] = ','.join(recipients)

    s = smtplib.SMTP(EMAIL_HOST, EMAIL_PORT)
    s.set_debuglevel(False)
    s.starttls()
    s.login(EMAIL_HOST_USER, EMAIL_HOST_PASSWORD)
    s.sendmail(SERVER_EMAIL, recipients, msg.as_string())
    s.quit()


def send_inspection_email(account_title, account_login, inspection_table_html):
    u"""
    Уведомление о проверке
    :param account_title:
    :param account_login:
    :param inspection_table_html:
    :return:
    """
    msg_subject = 'ВНИМАНИЕ! Назначена гос. проверка (%s)' % account_title
    msg_body = """
    <html>
    <head></head>
    <body>
    Данные из системы госпроверок: <br/>
    %(table)s <br/>
    Для входа в систему воспользуйтесь следующей ссылкой:
    <a href="yoxlama.gov.az">yoxlama.gov.az</a><br/>
    Наименование юрлица: %(title)s <br/>
    Логин (VOEN): %(login)s<br/>
    </body>
    </html>
    """ % {
        'title': account_title,
        'login': account_login,
        'table': inspection_table_html
    }
    send_mail(WARNING_RECIPIENTS, msg_subject, msg_body)


# Собственно, цикл проверки списка учетных записей на предмет проверок
for account_login, acc_data in ACCOUNTS.items():
    account_password = acc_data['password']
    account_title = acc_data['title']
    source_inspections_hash = acc_data['source_hash']

    # Вход в систему
    login_count = 0
    response = yoxlamalar_login(br, account_login, account_password)
    while response.code != 200 and login_count < 5:
        response = yoxlamalar_login(br, account_login, account_password)
        login_count += 1

    # После успешного входа в систему открытая страница - список проверок
    if response.code == 200:
        html_data = response.get_data()

        soup = BeautifulSoup(html_data, 'html.parser')
        inspections_table = soup.find('table', id='ctl00_ContentPlaceHolder1_grInspections_DXMainTable')

        # Получение текущего хэша таблицы проверок
        md5_hash = hashlib.md5()
        md5_hash.update(str(inspections_table))
        curr_inspections_hash = md5_hash.hexdigest()

        # print u'Учетная запись: %s' % account_login
        # print u'Эталонный хэш таблицы проверок: %s' % source_inspections_hash
        # print u'Текущий хэш таблицы проверок: %s' % curr_inspections_hash

        # Если хэш не совпал, то шлем сообщение о проверке
        if source_inspections_hash != curr_inspections_hash:
            send_inspection_email(account_title, account_login, str(inspections_table))
            # print u'Назначена проверка. Сообщение отправлено.'

        br.open(LOGOUT_URL)
    else:
        # Пять неудачных попыток проверить аккаунт на предмет проверок
        msg_text = 'После 5 попыток не удалось данные о госпроверках для %(title)s. Возвращен код статуса %(ret_code)d' % \
                   {'title': account_title, 'ret_code': response.code}
        send_mail(STAFF_RECIPIENTS, '[Система уведомления о госпроверках]', msg_text)
        # print msg_text
