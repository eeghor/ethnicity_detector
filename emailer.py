# for sending an email notification:
import smtplib
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart

from jinja2 import Environment, FileSystemLoader

import os
import json

import boto3

class EthnicityEmailer:

	def __init__(self):
		pass

	def send_sns(self, send_what, msg, s3credsfile='config/creds-s3.json'):
			
			# topic ARN sends to email_notifications by default; however, we may want to send error messages to
			# ETL_Notifications (case sensitive!)

			if send_what not in 'email error'.split():
				raise ValueError('[send_sns]: send_what argument must be either \'email\' or \'error\'!')

			topic_arn_ = [l.strip() for l in open('config/arn.txt','r').readlines() if l.strip()].pop()

			if send_what == 'email':

				boto3.client('sns', **json.load(open(s3credsfile))).publish(
									TopicArn=topic_arn_,
									Subject='new ethnicities', 
									Message=msg)

			elif send_what == 'error':
				
				boto3.client('sns', **json.load(open(s3credsfile))).publish(
									TopicArn=':'.join(topic_arn_.split(':')[:-1] + ['ETL_Notifications']),
									Subject='ethnicity', 
									Message=msg)
	
	def send_email_jinja(self, subj, template_maps, table_ref, creds_loc='local'):
		"""
		send a nice email about the top 3 detected ethnicities
		"""
		if creds_loc not in 'local env'.strip():
			raise('[send_email_jinja] creds_loc option must be either \'local\' or \'env\'!')

		if creds_loc == 'local':

			local_credfile = 'config/email.txt'

			try:	
				sender_email, sender_pwd, smtp_server, smpt_port, recep_emails = [line.split("=")[-1].strip() 
									for line in open(local_credfile).readlines() if line.strip()]
			except:

				if not os.path.exists(local_credfile):
					msg_ = f'email credential file {local_credfile} doesn\'t exist!'
				else:
					msg_ = f'email credential file {local_credfile} has something missing!'

				self.send_sns(send_what='error', msg=msg_, s3credsfile='config/creds-s3.json')

				raise IOError(msg_)

		elif creds_loc == 'env':

			required_envs = 'ETHNICITY_SENDER_EMAIL ETHNICITY_SENDER_PWD ETHNICITY_RECEPIENT_EMAIL'.split()

			# check if these are existent first

			for env_ in required_envs:

				try:
					os.environ[env_]
				except:
					raise KeyError(f'[send_email_jinja] there\'s no environmental variable called {env_}!')

			sender_email = os.environ['ETHNICITY_SENDER_EMAIL']

			if '@' not in sender_email:
				raise ValueError(f'[send_email_jinja] variable sender_email doesn\'t look like a proper email address!')

			sender_pwd = os.environ['ETHNICITY_SENDER_PWD']

			recep_emails = os.environ['ETHNICITY_RECEPIENT_EMAIL']

			for rec_email in recep_emails.split(';'):
				if rec_email.strip():
					if '@' not in rec_email:
						raise ValueError(f'[send_email_jinja] variable recep_emails seems to contain corrupted email addresses!')

			# note that there are 2 more variables that we need and in case the env option is chosen these
			# are hard-coded

			smtp_server = 'smtp.office365.com'
			smpt_port = '587'

	
		email = MIMEMultipart('related')   
		
		email['From'] = sender_email
		email['To'] = recep_emails
		email['Subject'] = subj
		
	
		file_loader = FileSystemLoader('templates')

		env = Environment(loader=file_loader)
		template = env.get_template('ethnicity_template.html')
	
		msg_text = template.render(**template_maps, eth_tab=table_ref)
	
		# main part is the Jinja2 template
		email.attach(MIMEText(msg_text,'html'))
	
		for i in range(1,4):
	
			p = template_maps[f'eth{i}']
	
			flag_pic = f'img/{p}.png'
			
			# if not national flag found, use Australian
			if not os.path.exists(f'templates/{flag_pic}'):
				flag_pic = 'img/australian.png'
	
			flag_pic_full = f'templates/{flag_pic}'
	
			fp = open(flag_pic_full, 'rb')
			msg_img = MIMEImage(fp.read())
			fp.close()
	
			msg_img.add_header('Content-ID', f'<{flag_pic}>')
			msg_img.add_header('Content-Disposition', 'inline', filename=f'{flag_pic}')
	
			email.attach(msg_img)
	
		server = smtplib.SMTP(smtp_server, smpt_port)
		server.starttls()

		print('trying to log into smtm server..', end='')
		try:
			server.login(sender_email, sender_pwd)
		except:
			raise Exception(f'can\'t log into your smtp server.. check credentials?')

		print('ok')

		server.sendmail(sender_email, [email.strip() for email in recep_emails.split(";")], email.as_string())
	
		server.quit()
	