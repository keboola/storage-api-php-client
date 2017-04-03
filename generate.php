<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 29/03/17
 * Time: 10:57
 */

echo '"id","name"' . "\n";

for ($i=0; $i < 100000; $i++) {
    echo sprintf('"%s","%s"' . "\n", rand(), rand());
}
