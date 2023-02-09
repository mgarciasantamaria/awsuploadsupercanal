#!/usr/bin/env python
#_*_ codig: utf8 _*_
import xml.etree.ElementTree as ET
import smtplib, threading, sys, os
from email.message import EmailMessage

#************************* readXML Function******************************#*
#funcion que permite leer un archivo xml y extraer la cuenta y el nombre #*
#de las imagenes y video asociados al xml.                               #*
# Se debe ingresar el siguiente parametro:                               #*
# xmlfile type str, ruta del archivo xml                                 #*
# Retorna diccionario con el nombre del video y las imagenes             #*
#************************************************************************#*
def readXML(xmlFile):                                                    #*
    i=0                                                                  #*
    key_list=['video']                                                   #*
    tree=ET.parse(xmlFile)                                               #*
    root=tree.getroot()                                                  #*
    video_Name=root.find('masterUrl').text                               #*
    video_Duration=root.find('duration').text                            #*
    values_List=[{'name':video_Name, 'duration':video_Duration}]         #*
    for image in root.iter('image'):                                     #*
        key_list.append('Imagen'+str(i))                                 #*
        values_List.append(image.find('url').text)                       #*
        i=i+1                                                            #*
    return dict(zip(key_list, values_List))                              #*
#************************************************************************#*

#********************** Organize Function *******************************#*
#Funcion que permite organizar la lista de contenidos de la forma video, #*
#imagenes y xml encontrados en el folder de cada paquete.                #*
# Se debe ingresar el siguiente parametro                                #*
#Files type lista, lista de files encontrados en el folder.              #*
#Retorna lista de files organizados                                      #*
#************************************************************************#*
def Organize(Files):                                                     #*
    for file in Files:                                                   #*
        if '.mp4' in file:                                               #* 
            Files.remove(file)                                           #*
            Files.insert(0, file)                                        #*
        elif '.jpg' in file:                                             #*
            pass                                                         #*
        elif '.xml' in file:                                             #*
            Files.remove(file)                                           #*
            Files.insert(len(Files), file)                               #*
        else:                                                            #*
            Files.remove(file)                                           #*
    return Files                                                         #*
#************************************************************************#*

#***********************SendMail Function*******************#*
#Funcion que permite enviar email                           #*
#Ingresar los siguientes parametros:                        #*
#test type string, texto del cuerpo del correo              #*
#Subject type string, asunto del correo                     #*
#No Retorna                                                 #*
#***********************************************************#*
def SendMail(text, Subject):                                #*
    msg = EmailMessage()                                    #*
    msg.set_content(text)                                   #*
    msg['Subject'] = Subject                                #*
    msg['From'] = 'alarmas-aws@vcmedios.com.co'             #*
    msg['To'] = ['mgarcia@vcmedios.com.co']           #*
    conexion = smtplib.SMTP(host='10.10.122.17', port=25)   #*
    conexion.ehlo()                                         #*
    conexion.send_message(msg)                              #*
    conexion.quit()                                         #*
    return                                                  #*
#***********************************************************#*

#********** Clase para observar el porcentaje de Upload ****************#*
#***********************************************************************#*
class ProgressPercentage(object):                                       #*                                                                        
    def __init__(self, filename):                                       #*
        self._filename = filename                                       #*
        self._size = float(os.path.getsize(filename))                   #*
        self._seen_so_far = 0                                           #*
        self._lock = threading.Lock()                                   #*
                                                                        #*
    def __call__(self, bytes_amount):                                   #*
        # To simplify, assume this is hooked up to a single filename    #*
        with self._lock:                                                #*
            self._seen_so_far += bytes_amount                           #*
            percentage = (self._seen_so_far / self._size) * 100         #*
            sys.stdout.write(                                           #*
                "\r%s  %s / %s  (%.2f%%)" % (                           #*
                    self._filename, self._seen_so_far, self._size,      #*
                    percentage))                                        #*
            sys.stdout.flush()                                          #*
#***********************************************************************#*