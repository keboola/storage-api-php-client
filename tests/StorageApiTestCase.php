<?php
/**
 *
 * User: Martin Halamíček
 * Date: 13.8.12
 * Time: 8:52
 *
 */

namespace Keboola\Test;

use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;

abstract class StorageApiTestCase extends \PHPUnit_Framework_TestCase
{
    const BACKEND_MYSQL = 'mysql';
    const BACKEND_REDSHIFT = 'redshift';
    const BACKEND_SNOWFLAKE = 'snowflake';

    const STAGE_IN = 'in';
    const STAGE_OUT = 'out';
    const STAGE_SYS = 'sys';

    protected $_bucketIds = array();

    /**
     * @var \Keboola\StorageApi\Client
     */
    protected $_client;

    public function setUp()
    {
        $this->_client = new \Keboola\StorageApi\Client(array(
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
        ));
    }


    protected function _initEmptyTestBuckets()
    {
        foreach (array(self::STAGE_OUT, self::STAGE_IN) as $stage) {
            $this->_bucketIds[$stage] = $this->initEmptyBucket('API-tests', $stage);
        }
    }

    /**
     * Init empty bucket test helper
     * @param $name
     * @param $stage
     * @return bool|string
     */
    private function initEmptyBucket($name, $stage)
    {
        try {
            $bucket = $this->_client->getBucket("$stage.c-$name");
            $tables = $this->_client->listTables($bucket['id']);
            foreach ($tables as $table) {
                $this->_client->dropTable($table['id']);
            }
            return $bucket['id'];
        } catch (\Keboola\StorageApi\ClientException $e) {
            return $this->_client->createBucket($name, $stage, 'Api tests');
        }
    }

    /**
     * @param $path
     * @return array
     */
    protected function _readCsv($path, $delimiter = ",", $enclosure = '"', $escape = '"')
    {
        $fh = fopen($path, 'r');
        $lines = array();
        while (($data = fgetcsv($fh, 1000, $delimiter, $enclosure, $escape)) !== false) {
            $lines[] = $data;
        }
        fclose($fh);
        return $lines;
    }

    public function assertLinesEqualsSorted($expected, $actual, $message = "")
    {
        $expected = explode("\n", $expected);
        $actual = explode("\n", $actual);

        sort($expected);
        sort($actual);
        $this->assertEquals($expected, $actual, $message);
    }

    public function assertArrayEqualsSorted($expected, $actual, $sortKey, $message = "")
    {
        $comparsion = function ($attrLeft, $attrRight) use ($sortKey) {
            if ($attrLeft[$sortKey] == $attrRight[$sortKey]) {
                return 0;
            }
            return $attrLeft[$sortKey] < $attrRight[$sortKey] ? -1 : 1;
        };
        usort($expected, $comparsion);
        usort($actual, $comparsion);
        return $this->assertEquals($expected, $actual, $message);
    }

    public function tableExportFiltersData()
    {
        return array(
            // first test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG'),
                    'columns' => ['id', 'name', 'sex'],
                ),
                array(
                    array(
                        "1",
                        "martin",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "female",
                    ),
                ),
            ),
            // first test with defined operator
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG'),
                    'whereOperator' => 'eq',
                ),
                array(
                    array(
                        "1",
                        "martin",
                        "PRG",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ),
                ),
            ),
            // second test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG', 'VAN')
                ),
                array(
                    array(
                        "1",
                        "martin",
                        "PRG",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ),
                    array(
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ),
                ),
            ),
            // third test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG'),
                    'whereOperator' => 'ne'
                ),
                array(
                    array(
                        "5",
                        "hidden",
                        "",
                        "male",
                    ),
                    array(
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ),
                    array(
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ),
                ),
            ),
            // fourth test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array('PRG', 'VAN'),
                    'whereOperator' => 'ne'
                ),
                array(
                    array(
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ),
                    array(
                        "5",
                        "hidden",
                        "",
                        "male",
                    ),
                ),
            ),
            // fifth test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array(''),
                    'whereOperator' => 'eq'
                ),
                array(
                    array(
                        "5",
                        "hidden",
                        "",
                        "male",
                    ),
                ),
            ),
            // sixth test
            array(
                array(
                    'whereColumn' => 'city',
                    'whereValues' => array(''),
                    'whereOperator' => 'ne'
                ),
                array(
                    array(
                        "4",
                        "miro",
                        "BRA",
                        "male",
                    ),
                    array(
                        "1",
                        "martin",
                        "PRG",
                        "male"
                    ),
                    array(
                        "2",
                        "klara",
                        "PRG",
                        "female",
                    ),
                    array(
                        "3",
                        "ondra",
                        "VAN",
                        "male",
                    ),
                ),
            ),
        );
    }

    protected function getTestBucketId($stage = self::STAGE_IN)
    {
        return $this->_bucketIds[$stage];
    }

    protected function createAndWaitForEvent(\Keboola\StorageApi\Event $event)
    {
        $id = $this->_client->createEvent($event);

        sleep(2); // wait for ES refresh
        $tries = 0;
        while (true) {
            try {
                return $this->_client->getEvent($id);
            } catch (\Keboola\StorageApi\ClientException $e) {
                echo 'Event not found: ' . $id . PHP_EOL;
            }
            if ($tries > 4) {
                throw new \Exception('Max tries exceeded.');
            }
            $tries++;
            sleep(pow(2, $tries));
        }
    }

    protected function createAndWaitForFile($path, FileUploadOptions $options, $sapiClient = null)
    {
        $client = $sapiClient ? $sapiClient : $this->_client;

        $fileId = $client->uploadFile($path, $options);
        return $this->waitForFile($fileId, $client);
    }

    protected function waitForFile($fileId, $sapiClient = null)
    {
        $client = $sapiClient ? $sapiClient : $this->_client;
        $fileSearchOptions = new ListFilesOptions();
        $fileSearchOptions = $fileSearchOptions->setQuery(sprintf("id:%s", $fileId));

        $tries = 0;
        sleep(2);
        while (true) {
            try {
                $files = $client->listFiles($fileSearchOptions);
                if (count($files) && $files[0]['id'] === $fileId) {
                    return $fileId;
                }
            } catch (\Keboola\StorageApi\ClientException $e) {
                echo 'File not found: ' . $fileId . PHP_EOL;
            }
            if ($tries > 4) {
                throw new \Exception('Max tries exceeded.');
            }
            $tries++;
            sleep(pow(2, $tries));
        }
    }
}
