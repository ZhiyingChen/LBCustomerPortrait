import win32com.client as win32
from datetime import datetime
from pathlib import Path
import os
import sys
import pythoncom


class send_email:
    def outlook(self, addressee, message_subject, message_body):
        pythoncom.CoInitialize()
        # print('message_subject in class:', message_subject)
        # print('message_body in class:', message_body)
        olMailItem = 0x0
        outlook = win32.Dispatch("outlook.Application")  # 固定写法
        mail = outlook.CreateItem(olMailItem)  # 固定写法
        # mail = outlook.CreateItem(0)
        sender = 'xxx@airproducts.com'
        mail.SendUsingAccount = sender
        mail.To = addressee  # 收件人
        # mail.CC = cc #抄送人
        # mail.Recipients.Add(addressee)
        mail.Subject = '{}'.format(message_subject)  # 邮件主题
        # 图片地址
        dirname, filename = os.path.split(os.path.abspath(sys.argv[0]))
        pic_path = os.path.join(dirname, "feedback.png")
        print(pic_path)
        # path_plot = "image.jpg"
        # attachment = mail.Attachments.Add(
        #     "C:\\Users\\zhoud8\\Documents\\OneDrive - Air Products and Chemicals, Inc\\python_project\\gui\\Forecasting\\image.png")
        # attachment.PropertyAccessor.SetProperty(
        #     "http://schemas.microsoft.com/mapi/proptag/0x3712001F", "MyId1")
        mail.HTMLBody = '''<html>
                  <body>
                  <p> {} </p>
                  <p> {} </p>
                  <p> {} </p>
                  <br><img src= "{}">
                 </body></html>'''.format(message_body[0], message_body[1],  message_body[2], pic_path)
        mail.Attachments.Add(pic_path)
        noSendNames = ['aaa', 'bbb', 'ccc']
        if not any(s in message_subject.upper() for s in noSendNames):
            mail.Send()  # 发送
        pythoncom.CoUninitialize()

    def getEmailData(self, result, reason):
        '''准备邮件内容'''
        now = datetime.now()
        refresh_time = now.strftime("%Y-%m-%d %H:%M")
        home = str(Path.home()).split('\\')
        if len(home) > 2:
            home_name = home[2]
        else:
            home_name = 'unknow person'
        # 发送信息
        message_subject = 'LB 调度 ' + home_name + ' | ' + refresh_time

        forecast_version = 'Forecast Version 1.0'
        # 发送信息
        message_body = ("Result:  {}".format(result),
                        "Reason:  {}".format(reason),
                        'Version: {}'.format(forecast_version))
        # 收件人邮箱列表
        recNames = [i+'@airproducts.com' for i in ['zhoud8', 'zhangy69', 'lid16']]
        addressee = ';'.join(recNames)
        return message_subject, message_body, addressee
