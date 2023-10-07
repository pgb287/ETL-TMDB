# ETL-TMDB
ETL realizado en el curso de Data Engineer de Coderhouse
Se realiza una peticion diaria a la API de la pagina de Peliculas TMDB la cual es accedida por medio de un token de seguridad. La solicitud refiere a las peliculas mas populares del dia, asociados con valores como ser cantidad de votos, puntaje promedio, entre otros. Los datos son llevados a un dataframe en la cual se realizan algunas transformaciones, por ejemplo seleccionar la pelicula con mayor puntaje de popularidad. Éste registro es almacenado en una BD en Amazon Redshift. Por ultimo se verifica si el puntaje de popularidad supera cierto limite, si esto sucede, se informa automaticamente por correo electronico al usuario.

Todo el proyecto se realizó en Python, utilizando Pandas, Docker y Airflow entre otras librerias y herramientas.

Adjunto algunas capturas:

![image](https://github.com/pgb287/ETL-TMDB/assets/44307296/d673c885-1c71-4544-b16f-477c587b527f)

![image](https://github.com/pgb287/ETL-TMDB/assets/44307296/78f69091-807c-4543-9401-2bed6ed1273c)

![image](https://github.com/pgb287/ETL-TMDB/assets/44307296/4708e825-a068-49ac-8c68-b9f7243195e5)


