# Gnoss.BackgroundTask.UserWall.OpenCORE

Aplicación de segundo plano que se encarga de generar la actividad reciente relativa a todas las comunidades que pertenece un usuario en su muro de la plataforma, habitualmente en la home de la plataforma.

Configuración estandar de esta aplicación en el archivo docker-compose.yml: 

```yml
userwall:
    image: userwall
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
     - ./logs/userwall:/app/logs
```

Se pueden consultar los posibles valores de configuración de cada parámetro aquí: https://github.com/equipognoss/Gnoss.Platform.Deploy
