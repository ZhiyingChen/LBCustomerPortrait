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


def a():
    subject_now = '【PPU】昨日执行情况提醒'
    # 全量发送
    # 'lid16@airproducts.com','xier2@airproducts.com','ZHAOL12@@airproducts.com'
    df_1 = df_1.drop(columns_to_drop, axis=1)
    html_table = df_1.to_html(index=False)
    outlook = win32.Dispatch("Outlook.Application")
    mail = outlook.CreateItem(0)
    mail.Subject = subject_now
    mail.HTMLBody = f"""
    <html>
    <head>
    <style>
            body, table {{ font-family: Arial, sans-serif; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; }}
            th {{ background-color: #f2f2f2; }}
    </style>
    </head>
    <body>
    <p>请注意，您的监控车辆【昨日关单】行程中以下提货不稳定(在地磅充装不稳定为实际满重与设定满重偏差大于60kg、不在地磅充装不稳定为实际与设定满重相差大于300kg)：(<a href='https://qliksense.america.apci.com/sense/app/b2c1c625-0d3a-4353-b1ae-b55743c3d21a/sheet/6a1b4e4e-0ac2-4ec3-bc2f-9c5227af1f50/state/analysis/bookmark/c4c2d8f0-b086-4a89-a88a-10107a9c2049' target='_blank'>更多内容可到qlik sense China Project X 流中PPU查看</a>)</p>
    {html_table}
    </body>
    </html>
        """
    mail.SentOnBehalfOfName = 'PROJECTX@airproducts.com'
    mail.Recipients.Add('lid16@airproducts.com')
    mail.Recipients.Add('ZHAOL12@airproducts.com')
    mail.Recipients.Add('PROJECTX@airproducts.com')

    mail.Save()
    mail.Send()