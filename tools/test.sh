#!/bin/bash

set -e

split_model() {
  local csv="$1" train test stats

  train="${csv/.csv/.train.csv}"
  test="${csv/.csv/.test.csv}"
  stats="${csv/.csv/.stats}"

  # 90% of the corpus to train the model
  echo "Writing $train..."
  sed '0~10d' < "$csv" > "$train"

  # 10% of the corpus to test the model
  echo "Writing $test..."
  sed -n '0~10p' < "$csv" > "$test"

  # stats
  echo "Writing $stats..."
  cut -f 2 < "$test" | sort | uniq -c | sort > "$stats"
}

generate_ngrams() {
  python ./tools/generate.py "$1" "$2"
}

test_sentences() {
  local pass=0 fail=0 sofar=0 total=0 proc=$1 finish=$2

  declare -A faillangs

  while IFS=$'\t' read -a fields; do
      real_lang=${fields[1]}
      text=${fields[2]}

      detected_lang=$(curl -s -G http://127.0.0.1:4242/api/detects/simple --data-urlencode "query=$text")
      if [ $? -ne 0 ]; then
          echo "Couldn't connect to Tatodetect daemon" >&2
          break
      fi

      if [ "$detected_lang" == "$real_lang" ]; then
          pass=$(($pass+1))
      else
          id=${fields[0]}
          printf "$id\t$real_lang\t%s\n" "$text" >> fail.log
          faillangs[$real_lang]=$(( ${faillangs[$real_lang]} + 1 ))
          fail=$(($fail+1))
      fi

      sofar=$(($sofar+1))
      if [ $(($sofar%100)) -eq 0 -o $sofar -eq 1 -o $sofar -eq $finish ]; then
          printf "%sworker $proc: $sofar/$finish%s" $(tput sc; tput cub 100; tput cuu $(($proc))) $(tput rc) >&2
      fi
  done

  for lang in "${!faillangs[@]}"; do
      echo faillang $lang ${faillangs[$lang]}
  done
  echo result $pass $fail
}

calc() {
  bc <<< "scale=2; $*"
}

run_test() {
  local csv="$1" nproc=$(grep -c processor /proc/cpuinfo)

  local total=$(( $(grep -c . "$csv") / $nproc ))
  for ((i=0; i<$nproc; i++)); do
    echo >&2
  done
  for i in $(seq 1 $nproc); do
    cat "$csv" | sed -n "$i~${nproc}p" | test_sentences $i $total &
  done
}

test_model() {
  local csv="$1"

  total_pass=0
  total_fail=0
  declare -A faillangs

  while read type arg1 arg2 arg3; do
    if [ $type == "result" ]; then
      pass=$arg1
      fail=$arg2
      total_pass=$(( $total_pass + $pass ))
      total_fail=$(( $total_fail + $fail ))
    elif [ $type == "faillang" ]; then
      lang=$arg1
      n=$arg2
      faillangs[$lang]=$(( ${faillangs[$lang]} + $n ))
    fi
  done <<< "$(run_test "$csv")"

  total=$(( $total_pass + $total_fail ))

  if [ $total_pass -gt 0 ]; then
      printf 'Accuracy: %s%% (pass=%d, fail=%d, total=%d)\n' $(calc "$total_pass*100/$total") $total_pass $total_fail $total
  fi

  if [ ${#faillangs[@]} -gt 0 ]; then
      echo "Wrote fail.log"
      stats="${sentences_csv/.csv/.stats}"
      if [ -r "$stats" ]; then
          for lang in "${!faillangs[@]}"; do
              echo "$lang ${faillangs[$lang]}"
          done | sort -k1 | join -1 2 -2 1 <(sort -k2 "$stats") - | awk '{print $1, $2, $3, $3*100/$2}' > fail.stats
          echo "Wrote fail.stats"
      fi
  fi
}

case $1 in
  split_model)
    shift
    split_model "$1"
    ;;

  generate_ngrams)
    shift
    generate_ngrams "$1" "$2"
    ;;

  test_model)
    shift
    test_model "$1"
    ;;

  *)
    echo "Usage: $0 split_model <sentences_detailed.csv>"
    echo "       => splits csv into training set and testing set"
    echo
    echo "       $0 generate_ngrams <sentences_detailed.train.csv> <ngrams.db>"
    echo "       => generates ngrams.db"
    echo
    echo "       $0 test_model <sentences_detailed.test.csv>"
    echo "       => tests model against an instance of Tatodetect"
    echo "          running on http://127.0.0.1:4242/"
    ;;
esac
