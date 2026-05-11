import os

def test_datalake_mount():
    # En el contenedor, el datalake se monta en la raíz /datalake
    datalake_path = "/datalake"
    print(f"Comprobando acceso al datalake en: {datalake_path}")
    
    if os.path.exists(datalake_path):
        print(f"Exito: {datalake_path} es accesible.")
        
        # Listar el contenido
        try:
            contents = os.listdir(datalake_path)
            print(f"Contenido detectado: {contents}")
            
            # Verificar carpetas esperadas
            expected = ['raw', 'cleanse', 'curated']
            for folder in expected:
                if folder in contents:
                    print(f"  - Carpeta encontrada: {folder}")
                else:
                    print(f"  - Aviso: No se encontró la carpeta '{folder}'")
            
            # Probar permisos de escritura en 'raw'
            test_file = os.path.join(datalake_path, "raw", ".test_mount")
            try:
                with open(test_file, "w") as f:
                    f.write("Test de montaje exitoso")
                print(f"Permisos de escritura verificados en /datalake/raw")
                os.remove(test_file) # Limpiar el archivo de prueba
            except Exception as e:
                print(f"Error al intentar escribir: {e}")
                
        except Exception as e:
            print(f"Error al listar el contenido: {e}")
    else:
        print(f"Error: {datalake_path} no existe dentro del contenedor.")
        print("IMPORTANTE: Asegúrate de haber reconstruido el contenedor (Rebuild Container) para que el montaje se active.")

if __name__ == "__main__":
    test_datalake_mount()
