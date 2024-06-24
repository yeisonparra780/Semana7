import os
import msal
import requests
import time
import conexionBd as con

# Configuraciones de la aplicación registrada en Azure AD
CLIENT_ID = 'tu_client_id'
CLIENT_SECRET = 'tu_client_secret'
TENANT_ID = 'tu_tenant_id'
AUTHORITY = f'https://login.microsoftonline.com/{TENANT_ID}'
SCOPES = ['https://graph.microsoft.com/.default']

# Ruta donde se guardarán los archivos descargados
DOWNLOAD_PATH = 'ruta/a/la/carpeta/de/descargas'

# Función para obtener un token de acceso
def get_access_token():
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=AUTHORITY,
        client_credential=CLIENT_SECRET
    )

    result = app.acquire_token_for_client(scopes=SCOPES)

    if 'access_token' in result:
        return result['access_token']
    else:
        raise Exception('No se pudo obtener el token de acceso')

# Función para obtener el último correo con un archivo adjunto
def get_latest_email_with_attachment():
    access_token = get_access_token()
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    endpoint = 'https://graph.microsoft.com/v1.0/me/messages?$filter=hasAttachments eq true&$orderby=receivedDateTime desc&$top=1'

    response = requests.get(endpoint, headers=headers)
    if response.status_code == 200:
        emails = response.json()
        if emails['value']:
            return emails['value'][0]
        else:
            print("No hay correos con archivos adjuntos.")
    else:
        print(f"Error: {response.status_code}")
        print(response.json())
    
    return None

# Función para descargar archivos adjuntos de un correo específico
def download_attachments(email):
    access_token = get_access_token()
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    message_id = email['id']
    endpoint = f'https://graph.microsoft.com/v1.0/me/messages/{message_id}/attachments'

    response = requests.get(endpoint, headers=headers)
    if response.status_code == 200:
        attachments = response.json()
        for attachment in attachments['value']:
            if '@odata.type' in attachment and attachment['@odata.type'] == '#microsoft.graph.fileAttachment':
                file_name = attachment['name']
                file_content = attachment['contentBytes']
                file_path = os.path.join(DOWNLOAD_PATH, file_name)
                with open(file_path, 'wb') as f:
                    f.write(file_content.encode('latin1'))  # Adjust encoding if necessary
                print(f"Archivo descargado: {file_name}")
    else:
        print(f"Error: {response.status_code}")
        print(response.json())
        
def crear_lote(lote):
    try:
        print("Creando lote")
        document = {"nombre": "Juan", "edad": 30, "ciudad": "Madrid"}
        crear_lote = con.insertar_registro(document)
        print("Lote creado")
    except Exception as msg_error:             
        print(msg_error)

# Función principal para ejecutar el proceso de descarga
def main():
    
    lote = crear_lote(123)
    if not os.path.exists(DOWNLOAD_PATH):
        os.makedirs(DOWNLOAD_PATH)

    email = get_latest_email_with_attachment()
    if email:
        download_attachments(email)

while True:
    if __name__ == "__main__":
        main()
        time.sleep(60)

