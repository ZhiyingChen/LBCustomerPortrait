import datetime
import win32com.client as win32


def outlook_sender(
        sender: str,
        addressee: str,
        message_subject: str,
        message_body: str
):

    olMailItem = 0x0
    outlook = win32.Dispatch("outlook.Application")  # 固定写法
    mail = outlook.CreateItem(olMailItem)  # 固定写法
    mail.SendUsingAccount = sender
    mail.To = addressee  # 收件人
    mail.Subject = '{}'.format(message_subject)  # 邮件主题

    mail.HTMLBody = '''<html>
                      <body>
              <p>   Hi all ，</p>
              <p> Current condition of downloading issues:  </p>
              <p> {} </p>
             </body></html>'''.format(message_body)
    mail.Send()  # 发送



def send_email(
        addressee : str,
        sender: str
):
    message_subject = "Today's Report during LB Forecast Data"
    now = datetime.datetime.now()
    message_body = "LB Forecast Data data updated at {}".format(now.strftime("%Y-%m-%d, %H:%M:%S"))
    outlook_sender(sender, addressee, message_subject, message_body)

