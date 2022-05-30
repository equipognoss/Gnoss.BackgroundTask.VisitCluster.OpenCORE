# Gnoss.BackgroundTask.VisitCluster.OpenCORE

Aplicación de segundo plano que se encarga de insertar en base de datos las visitas que ha contabilizado el servicio Visit Registry. Espera un tiempo especificado (por defecto 5 minutos) para registrar las visitas transcurridas en ese período de tiempo. 

Configuración estandar de esta aplicación en el archivo docker-compose.yml: 

```yml
visitcluster:
    image: visitcluster
    env_file: .env
    environment:
     virtuosoConnectionString: ${virtuosoConnectionString}
     acid: ${acid}
     base: ${base}
     RabbitMQ__colaServiciosWin: ${RabbitMQ}
     RabbitMQ__colaReplicacion: ${RabbitMQ}
     redis__redis__ip__master: ${redis__redis__ip__master}
     redis__redis__bd: ${redis__redis__bd}
     redis__redis__timeout: ${redis__redis__timeout}
     redis__recursos__ip__master: ${redis__recursos__ip__master}
     redis__recursos__bd: ${redis__recursos_bd}
     redis__recursos__timeout: ${redis__recursos_timeout}
     redis__liveUsuarios__ip__master: ${redis__liveUsuarios__ip__master}
     redis__liveUsuarios__bd: ${redis__liveUsuarios_bd}
     redis__liveUsuarios__timeout: ${redis__liveUsuarios_timeout}
     idiomas: "es|Español,en|English"
     Servicios__urlBase: "https://servicios.test.com"
     connectionType: "0"
     intervalo: "100"
    volumes:
     - ./logs/visitcluster:/app/logs
     - ./visitcluster/:/app/elementos
```

Se pueden consultar los posibles valores de configuración de cada parámetro aquí: https://github.com/equipognoss/Gnoss.Platform.Deploy
