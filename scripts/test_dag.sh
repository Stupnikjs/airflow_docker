
my_bash_func(){

    local dag_id="$1"
    
    # grep '^[a-z]': Filters out the header line that starts with an uppercase letter, leaving only the container lines. 
    # awk '{print $1}': Prints the first column (container ID) of each line.
    CONTAINER_ID=$(docker ps | grep '^[a-z]' | awk '{print $1}' | head -n 1 | tail -n 1)
    docker exec -it $CONTAINER_ID bash -c "airflow dags trigger  $dag_id "
   
    # "airflow tasks run $dag_id $task_id $today"
    

}


# 