#!/usr/bin/env python
#_*_ codig: utf8 _*_
import os, sys, traceback, datetime, time
import boto3
from Modules.functions import * 
from Modules.Constants import *

if __name__ == '__main__':
#---Se inicia el contador de files subidos en 0
    Counter=0
    Count_images=0
    Count_videos=0
    Count_xmls=0
#---Se estable el perfil a utilizar para crear conexion con aws
    aws_session=boto3.Session(profile_name='pythonapps')
#---Se establece conexion con el servicio S3 de AWS
    s3=aws_session.client('s3')

#---Inicio del cliclo infinito    
    while True:
        try:
#-----------Variable que registra la cuenta de files al comenzar el siguinete ciclo
            Counter_Before=Counter
#-----------Lista lista de folder correspondientes a los canales            
            folders_Channels=os.listdir(source_Path)
#-----------Se recorre la lista de canales uno por uno            
            for channel in folders_Channels:
#---------------Lista de paquetes VOD del canal actual
                VOD_Packages=os.listdir(f"{source_Path}/{channel}")
#---------------Pregunta si la lista de de paquetes es vacia
                if VOD_Packages == []:
                    print(f"{channel} Empty channel folder")
                    continue
                else:
                    print(f"{channel} {len(VOD_Packages)} Package found")
#-------------------lista de los files correspondientes al paquete seleccionado
                    for VOD_Pack in VOD_Packages:
#-----------------------lista de files dentro del paquete
                        Files=os.listdir(f"{source_Path}/{channel}/{VOD_Pack}")
                        if Files == []:
                            print(f"\t Empty package")
                        else:
    #-----------------------Se organizan los files con la funcion personalizada Organize() en el orden video, imagenes y xml
                            Files=Organize(Files)
    #-----------------------Recore la lista files uno por uno
                            print(f"{channel}/{VOD_Pack} {len(Files)} Files found")
                            for File in Files:
    #---------------------------Se establece la ruta del file selecionado
                                file_Path=f"{source_Path}/{channel}/{VOD_Pack}/{File}"
                                print(f"{channel}/{VOD_Pack}/{File} Select")
    #---------------------------Pregunta si el file es un xml
                                if '.xml' in File:
    #-------------------------------Pregunta si el file existe en la nube 
                                    if s3.list_objects_v2(Bucket='bucket-in-companion-tbx', Prefix=f"{channel}/{VOD_Pack}/{File}")['KeyCount'] != 0:
                                        print(f"\tit exists in the bucket\n\tremoving files")
    #-----------------------------------Si el archivo existe se en listan todos los archivos del paquete                                    
                                        Del_Files=os.listdir(f"{source_Path}/{channel}/{VOD_Pack}")
    #-----------------------------------Se recore la lista de files para ser eliminados uno a uno, si el xml ha sido subido quiere decir que todos los demas files tambien
                                        for Del_File in Del_Files:
    #---------------------------------------Se establece la ruta del file selecionado
                                            file_Delete_Path=f"{source_Path}/{channel}/{VOD_Pack}/{Del_File}"
    #---------------------------------------Se elimina el file selecionado
                                            os.remove(file_Delete_Path)
    #-----------------------------------Se elimina el folder del paquete una vez esta vacio 
                                        os.rmdir(f"{source_Path}/{channel}/{VOD_Pack}")
    #-----------------------------------Se sale del cilco for actual que recorre los files del paquete selecionado
                                        break    
                                    else:
                                        print(f"\tdoes not exist in bucket\n\textracting data")
    #-----------------------------------Si el xml no existe en la nube se extraen los datos del mismo con la funcion personalizada readXML 
                                        Dictionary=readXML(file_Path)
    #-----------------------------------Se establece la cantidad de files correspondientes al paquete selecionado segun el xml
                                        CantXML=len(Dictionary)
    #-----------------------------------Se establece el contador de files en la base de datos correspondiente al paquete seleccionado en 0
                                        CantDB=0
    #-----------------------------------Se secorre la lista de files segun los datos extraidos del xml
                                        for Contend in Dictionary:
    #---------------------------------------Preguta si el file seleccionado de la lista corresponde a un video
                                            if Contend == 'video':
    #-------------------------------------------Pregunta si existe registro en la base de datos del file selecionado
                                                if s3.list_objects_v2(Bucket='bucket-in-companion-tbx', Prefix=f"{channel}/{VOD_Pack}/{Dictionary['video']['name']}")['KeyCount'] != 0:
    #-----------------------------------------------Si existe quiere decir que el archivo ya esta en nube y se aumenta el contador en 1
                                                    CantDB+=1
                                                else:
    #-----------------------------------------------Si no existe se sale del ciclo for que recorre la lista de files segun el xml
                                                    break
    #---------------------------------------Pregunta si el file seleccionado actualmente corresponde a una imagen
                                            elif 'Imagen' in Contend: 
    #-----------------------------------------------Valida si existe registro en la base de datos del file selecionado
                                                    if s3.list_objects_v2(Bucket='bucket-in-companion-tbx', Prefix=f"{channel}/{VOD_Pack}/{Dictionary[Contend]}")['KeyCount'] != 0:
    #---------------------------------------------------Si existe registro se aumenta el contador en 1
                                                        CantDB+=1
    #---------------------------------------Si el file no es video o imagen 
                                            else:
    #-------------------------------------------Valida si elcontador de files subidos es diferente de cero y si en el cliclo actual de while no se ha subido ningun otro file
                                                if Counter!=0 and Counter==Counter_Before:
    #-----------------------------------------------Se crea archivo para adjuntar al email
                                                    log_file=open("log.txt","w")
                                                    log_file.write(f"{file_Path}: Are finded inconsistancies in this file. Has not uploaded to AWS S3\n")
                                                    log_file.close()
    #-----------------------------------------------Se establece la bandera en true para que el modulo sendmail adjunte el archivo de texto
                                                    flag_log=True
                                                    pass
    #-------------------------------------------Se sale del actual ciclo for 
                                                break
    #-----------------------------------Pregunta si la cantidad de files registrados en la base de datos es igual a la cantidad de files extraidos del xml
                                        if CantDB==CantXML:
    #---------------------------------------Se establece la ruta en ek bucket de subir el file
                                            object_name=f"{channel}/{VOD_Pack}/{File}"
                                            try:
                                                print(f"\tuploading file")
    #-------------------------------------------Se utiliza el metodo para subir el archivo al bucket
                                                s3.upload_file(file_Path, Bucket, object_name, Callback=ProgressPercentage(file_Path))
    #-------------------------------------------Aumenta en uno el contador de files subidos a S3
                                                Count_xmls+=1
                                                Counter+=1
                                            except:
    #-------------------------------------------Captura del error del sistema
                                                error=sys.exc_info()[2]
                                                error_Info=traceback.format_tb(error)[0]
    #-------------------------------------------Se establece el asunto del correo a enviar
                                                text_Mail=f"{object_name} Failed to upload to AWs S3 bucket.\n{str(sys.exc_info()[1])}\n{error_Info}"
                                                print(text_Mail)
    #-------------------------------------------Se envia email
                                                SendMail(text_Mail, Subject='Warning S3Upload')
    #-------------------------------------------Continua con el siguiente cilco del for                                            
                                                continue
    #-----------------------------------Si no se cumplen las validaciones anteriores continua con la siguiente linea
                                        else:
                                            pass
    #---------------------------Valida si el file es un video .mp4
                                elif '.mp4' in File:
    #-------------------------------Verifica si hay registro del file en la base de datos
                                    if s3.list_objects_v2(Bucket='bucket-in-companion-tbx', Prefix=f"{channel}/{VOD_Pack}/{File}")['KeyCount'] != 0:
                                        print(f"\tit exists in the bucket")
    #-----------------------------------pasa a la siguiente linea sin subirlo a bucket
                                        pass
                                    else:
    #-----------------------------------Se establece la ruta para subir el file al bucket
                                        object_name=f"{channel}/{VOD_Pack}/{File}"
                                        try:
                                            print(f"\tUploading file")
    #---------------------------------------Se utiliza el metodo para subir el archivo al bucket
                                            s3.upload_file(file_Path, Bucket, object_name, Callback=ProgressPercentage(file_Path))
    #---------------------------------------Aumenta en 1 el contador de files subidos a S3
                                            Count_videos+=1
                                            Counter+=1
                                        except:
    #---------------------------------------Captura del error que arroja el sistema
                                            error=sys.exc_info()[2]
                                            error_Info=traceback.format_tb(error)[0]
    #---------------------------------------Asunto del correo a enviar
                                            text_Mail=f"{object_name} Failed to upload to AWs S3 bucket.\n{str(sys.exc_info()[1])}\n{error_Info}"
                                            print(text_Mail)
    #---------------------------------------Se envia correo de alerta
                                            SendMail(text_Mail, Subject='Warning S3Upload')
                                            continue
    #---------------------------Pregunta si el mail es una imagen jpg
                                elif '.jpg' in File:
    #-------------------------------Verifica en la base de datos si existe registro del file
                                    if s3.list_objects_v2(Bucket='bucket-in-companion-tbx', Prefix=f"{channel}/{VOD_Pack}/{File}")['KeyCount'] != 0:
                                        print(f"\tit exists in the bucket")
    #-----------------------------------Si hay registro en la base de datos continua sin subir el archivo
                                        pass
                                    else:
    #-----------------------------------Si no hay registro en la base de datos  se establece la ruta de destino
                                        object_name=f"{channel}/{VOD_Pack}/{File}"
                                        try:
                                            print(f"\tUploading file")
    #---------------------------------------Se utiliza el metodo upload para subir el archivo al bucket
                                            s3.upload_file(file_Path, Bucket, object_name, Callback=ProgressPercentage(file_Path))
    #---------------------------------------Aumenta el contador de files subidos a S3 en 1
                                            Count_images+=1
                                            Counter+=1
                                        except:
    #---------------------------------------Captura del error que arroja el sistema
                                            error=sys.exc_info()[2]
                                            error_Info=traceback.format_tb(error)[0]
    #---------------------------------------Asunto del mail a enviar
                                            text_Mail=f"{object_name} Failed to upload to AWs S3 bucket.\n{str(sys.exc_info()[1])}\n{error_Info}"
                                            print(text_Mail)
    #---------------------------------------Envio del correo de alerta
                                            SendMail(text_Mail, Subject='Warning S3Upload')
                                            continue
                                else:
    #-------------------------------Continua con el ciclo si el file no corresponde a los esperados
                                    pass
#-----------Pregunta si el contador es diferente de cero y si el contador es igual al anterior ciclo
            if Counter!=0 and Counter==Counter_Before:
#---------------Cuerpo del correo a enviar
                text_log=f"Total videos uploaded: {Count_videos}\nTotal Images uploaded: {Count_images}\nTotal Xmls uploaded: {Count_xmls}"
                print(f"{text_log}\nEND!\n")
#---------------Se envia email de alerta
                SendMail(text_log, Subject='Supercanal Packages Upload Sumary')
#---------------Regresa el contador de files registrados a cero
                Counter=0
                Count_videos=0
                Count_images=0
                Count_xmls=0
#-----------Pregunta si el contador de files registrados es cero, esto indica que el codigo puede finalizar
            elif Counter==0:
                print(f"{datetime.datetime.strftime(datetime.datetime.now(), '%H:%M:%S')} Waiting 5 minutes")
#---------------Espera 5 minutos para volver a ejecutar el codigo
                time.sleep(300)
            else:
                pass  
        except:
#-----------Captura error del sistema
            error=sys.exc_info()[2]
            error_Info=traceback.format_tb(error)[0]
#-----------Texto a imprimir en el log
            text_log=f"Total videos uploaded: {Count_videos}\nTotal Images Uploaded: {Count_images}\nTotal Xmls uploaded: {Count_xmls}"
            print(text_log)
            text_Mail=f"An error occurred while executing the awsupload application on the RUNAPPSPROD server (10.10.130.39)\nTraceback info: {error_Info}\nError_Info:{str(sys.exc_info()[1])}\n\n"+text_log
            SendMail(text_Mail,"Execution Error Code")
            print(text_Mail)
#-----------interumpe la ejecucion del codigo
            quit()
