#! /bin/bash
basepath=$(cd `dirname $0`; pwd)
codePATH=PythonHelpServer.py
str="$basepath/$codePATH"
echo "$str"
while :
do
case "$(pgrep -f "python $str" | wc -w)" in
0)  echo "Starting program:     $(date)" >>pyserverlog/log
    python $codePATH &    
    ;;
*)  echo "running    $(pgrep -f "python $str" | wc -w)           $(date)" >> pyserverlog/log
    ;;
  
esac
sleep 10
done
