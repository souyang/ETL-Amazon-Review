FOR /F "tokens=*" %%i IN ('docker ps -q --filter "ancestor=postgres:13"') DO (
    docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" %%i
)