#!/usr/bin/python3
# -*- coding: utf-8 -*-

'''
Created on 27/02/2020

@author: ecrespo
@version: 1.0
@modified date: 13/05/2020
@description: Emerson UPS Data Collection store in MySQL DB and publish in MQTT Broker
UPS Emerson Network Power NXC - IntelliSlot Web Card

Libreria:
Para SNMP uso la libreria pysnmp
http://snmplabs.com/pysnmp/index.html
'''

import time

# --------------------------------------------------------------------------- # 
# PySNMP is a cross-platform, pure-Python SNMP engine implementation
# Instalación: pip install pysnmp
# Documentación librería pysnmp: http://pysnmp.sourceforge.net/
# --------------------------------------------------------------------------- #
from pysnmp.hlapi import *

# --------------------------------------------------------------------------- # 
# configure BBDD
# instalar como administrador: pip install mysql-connector-python
# --------------------------------------------------------------------------- # 
import mysql.connector as my_dbapi

# --------------------------------------------------------------------------- # 
# Conexión con MQTT
# pip install paho-mqtt
# https://www.eclipse.org/paho/clients/python/
# https://pypi.org/project/paho-mqtt/
# --------------------------------------------------------------------------- # 
import paho.mqtt.client as mqtt

broker_address="10.1.1.10"

# --------------------------------------------------------------------------- # 
# Mandar email
# smptlib is part of python's standard library, so you do not have to install it.
#
# Installar email:
# easy_install --upgrade setuptools
# pip install email
# o en windows usar easy_install email
# --------------------------------------------------------------------------- # 
import smtplib
from email.mime.text import MIMEText

destinatario = ['email@domain.com']

# --------------------------------------------------------------------------- #
# Funciones: envio de correo (no manda correos esta version
# --------------------------------------------------------------------------- #

def manda_correo(subject, texto, toaddr):
    msg = MIMEText(texto)
    fromaddr = "email@domain.com"
    msg['From'] = fromaddr
    msg['To'] = ",".join(toaddr)
    msg['Subject'] = subject
    s = smtplib.SMTP('10.1.1.11')
    # Next, log in to the server
    # s.login("youremailusername", "password")
    s.sendmail(fromaddr, toaddr, msg.as_string())
    s.quit()
    print(subject)
    print(texto)

UPSs = {'UPS_1':'10.1.1.12','UPS_2':'10.1.1.13'}

dict_datos = {}

datos = ['status','battery_temp','input_frec','output_frec','input_I_A','input_I_B','input_I_C','input_V_A','input_V_B','input_V_C'\
         ,'input_FP_A','input_FP_B','input_FP_C','output_I_A','output_I_B','output_I_C','output_V_A','output_V_B','output_V_C'\
         ,'output_FP_A','output_FP_B','output_FP_C','total_output_power','total_output_aparent_power','output_power_A_percentage'\
         ,'output_power_B_percentage','output_power_C_percentage']

# Obtener datos por SNMP
for UPS in UPSs:
    print("------------ " + UPS + " ------------")
    i = 0
    try:
        g = getCmd(SnmpEngine(), CommunityData('public'),UdpTransportTarget((UPSs[UPS], 161)), ContextData(),
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4123')),  #The operating status for the system
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4156')),  #The battery temperature for a cabinet
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4105')), #The system input frequency
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4207')), #The system output frequency
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4113')),  #The system input RMS current for Phase A    
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4114')),  #The system input RMS current for Phase B  
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4115')),  #The system input RMS current for Phase C
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4096')),  #The system input RMS voltage between phases A and Neutral
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4098')),  #The system input RMS voltage between phases B and Neutral
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4100')),  #The system input RMS voltage between phases C and Neutral
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4116')),  #The system input power factor of phase A
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4117')),  #The system input power factor of phase B
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4118')))  #The system input power factor of phase C
        errorIndication, errorStatus, errorIndex, varBinds = next(g)

        if errorIndication:
            print(errorIndication)
        elif errorStatus:
            print('%s at %s' % (errorStatus.prettyPrint(),
                                errorIndex and varBinds[int(errorIndex) - 1][0] or '?'))
        else:
            for varBind in varBinds:
                print(' = '.join([x.prettyPrint() for x in varBind]),datos[i])
                dict_datos[datos[i]]= varBind[1].prettyPrint()
                i = i + 1
    except:
        print("Error de conexión")

# More data
for UPS in UPSs:
    print("------------ " + UPS + " ------------")
    try:
        g = getCmd(SnmpEngine(), CommunityData('public'),UdpTransportTarget((UPSs[UPS], 161)), ContextData(),
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4204')),  #The system output RMS current for Phase A    
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4205')),  #The system output RMS current for Phase B  
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4206')),  #The system output RMS current for Phase C
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4385')),  #The system output RMS voltage between phases A and Neutral
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4386')),  #The system output RMS voltage between phases B and Neutral
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4387')),  #The system output RMS voltage between phases C and Neutral
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4210')),  #The system output power factor of phase A
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4211')),  #The system output power factor of phase B
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4212')),  #The system output power factor of phase C
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4208')),  #The sum total power of all system output phases
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4209')),  #The sum total aparent power of all system output phases
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4223')),  #The system output power on phase A as a percentage of the rated capacity
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4224')),  #The system output power on phase B as a percentage of the rated capacity
                ObjectType(ObjectIdentity('.1.3.6.1.4.1.476.1.42.3.9.20.1.20.1.2.1.4225')))  #The system output power on phase C as a percentage of the rated capacity
        errorIndication, errorStatus, errorIndex, varBinds = next(g)

        if errorIndication:
            print(errorIndication)
        elif errorStatus:
            print('%s at %s' % (errorStatus.prettyPrint(),
                                errorIndex and varBinds[int(errorIndex) - 1][0] or '?'))
        else:
            for varBind in varBinds:
                print(' = '.join([x.prettyPrint() for x in varBind]),datos[i])
                dict_datos[datos[i]]= varBind[1].prettyPrint()
                i = i + 1
    except:
        print("Error de conexión")

#conexión a BBDD
try: 
    cnx_my = my_dbapi.connect(user='user', password='password', host='server', database='database') #localhost en producción
    cursor_my = cnx_my.cursor()

    estado = dict_datos['status']
    dict_datos['status'] = "\'" + dict_datos['status'] + "\'" #pongo comillas a los strings

    query_estado = "SELECT status FROM `Tabla` ORDER BY `fecha` DESC LIMIT 1"
    cursor_my.execute(query_estado)
    myresult = cursor_my.fetchone()

    if (myresult[0] != estado):
        print("cambio estado")
        texto = "Se ha producido un cambio de estado en la UPS de '" + myresult[0] + "' a '" + estado + "'. Comprobar UPS"
        manda_correo("Cambio estado UPS ", texto, destinatario)
        
    
    query_my = "INSERT INTO Tabla (" + ','.join(x for x in datos) + ") VALUES (" + ','.join(dict_datos[x] for x in datos) + ")"

    cursor_my.execute(query_my)
    cnx_my.commit()
    cnx_my.close()
    
except:
    print("Error BBDD")
    manda_correo("Error inserción datos UPS ", "No es posible grabar datos en BBDD", destinatario)

#funciones de callback para MQTT
def on_connect(mqttc, obj, flags, rc):
    print("rc: " + str(rc))


def on_message(mqttc, obj, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))


def on_publish(mqttc, obj, mid):
    print("mid: " + str(mid))
    pass


def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_log(mqttc, obj, level, string):
    print(string)

def on_disconnect(mqttc, userdata,rc=0):
    print("Desconectado")
    logging.debug("DisConnected result code "+str(rc))
    mqttc.loop_stop()

#publicar en mosquitto
client = mqtt.Client() #create new instance
#client.enable_logger()
client.username_pw_set("user", "password")
#activo las funciones de callback
client.on_message = on_message
client.on_connect = on_connect
client.on_publish = on_publish
client.on_subscribe = on_subscribe
# Uncomment to enable debug messages
# mqttc.on_log = on_log

client.loop_start() #comienza el loop y lo paro en el disconnect
print("Conectando al broker...")
client.connect(broker_address) #connect to broker
time.sleep(2) #espero a que conecte al broker

for x in datos:
    topic = "UPS/" + x
    dato = dict_datos[x]
    client.publish(topic,dato,qos=0,retain=False)#publish

time.sleep(5)#espero a que todo publique

#client.loop_forever()
client.disconnect() #mando disconect

