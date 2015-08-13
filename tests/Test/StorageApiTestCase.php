<?php
/**
 *
 * User: Martin Halamíček
 * Date: 13.8.12
 * Time: 8:52
 *
 */

class StorageApiTestCase extends \PHPUnit_Framework_TestCase
{
	const BACKEND_MYSQL = 'mysql';
	const BACKEND_REDSHIFT = 'redshift';

	const STAGE_IN = 'in';
	const STAGE_OUT = 'out';
	const STAGE_SYS = 'sys';

	protected $_bucketIds = array();

	/**
	 * @var Keboola\StorageApi\Client
	 */
	protected $_client;

	public function setUp()
	{
		$this->_client = new Keboola\StorageApi\Client(array(
			'token' => STORAGE_API_TOKEN,
			'url' => STORAGE_API_URL,
			'backoffMaxTries' => 1,
		));
	}

	/**
	 * Init empty bucket test helper
	 * @param $name
	 * @param $stage
	 * @return bool|string
	 */
	protected function _initEmptyBucket($name, $stage, $backend = 'mysql')
	{
		try {
			$bucket = $this->_client->getBucket("$stage.c-$name");
			$tables = $this->_client->listTables($bucket['id']);
			foreach ($tables as $table) {
				$this->_client->dropTable($table['id']);
			}

			if ($bucket['backend'] != $backend) {
				$this->_client->dropBucket($bucket['id']);
				return $this->_client->createBucket($name, $stage, 'Api tests', $backend);
			}
			return $bucket['id'];
		} catch (\Keboola\StorageApi\ClientException $e) {
			return $this->_client->createBucket($name, $stage, 'Api tests', $backend);
		}
	}

	protected function _initEmptyBucketsForAllBackends()
	{
		foreach ($this->backends() as $backend) {
			foreach (array(self::STAGE_OUT, self::STAGE_IN) as $stage) {
				$this->_bucketIds[$stage . '-' . $backend[0]] = $this->_initEmptyBucket('API-tests-' . $backend[0], $stage, $backend[0]);
			}
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
		while (($data = fgetcsv($fh, 1000, $delimiter, $enclosure, $escape)) !== FALSE) {
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

	public  function assertArrayEqualsSorted($expected, $actual, $sortKey, $message = "")
	{
		$comparsion = function($attrLeft, $attrRight) use($sortKey) {
			if ($attrLeft[$sortKey] == $attrRight[$sortKey]) {
				return 0;
			}
			return $attrLeft[$sortKey] < $attrRight[$sortKey] ? -1 : 1;
		};
		usort($expected, $comparsion);
		usort($actual, $comparsion);
		return $this->assertEquals($expected, $actual, $message);
	}

	public function backends()
	{
		return array(
			array('mysql'),
			array('redshift'),
		);
	}

	public function tableExportFiltersData()
	{
		return array(
			// first test
			array(
				array(
					'whereColumn' => 'city',
					'whereValues' => array('PRG')
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

	protected function getTestBucketId($stage = self::STAGE_IN, $backend = self::BACKEND_MYSQL)
	{
		return $this->_bucketIds[$stage . '-' . $backend];
	}

	/**
	 * Prepends backend for each testing data
	 * @param $data
	 * @return array
	 */
	protected function dataWithBackendPrepended($data)
	{
		$backends = $this->backends();
		$return = array();
		foreach ($data as $row) {
			foreach ($backends as $backend) {
				$return[] = array_merge(array($backend[0]), $row);
			}
		}
		return $return;
	}

	protected function createAndWaitForEvent(\Keboola\StorageApi\Event $event)
	{
		$id = $this->_client->createEvent($event);

		sleep(2); // wait for ES refresh
		$tries = 0;
		while (true) {
			try {
				$this->_client->getEvent($id);
				return $id;
			} catch(\Keboola\StorageApi\ClientException $e) {
				echo 'Event not found: ' . $id . PHP_EOL;
			}
			if ($tries > 4) {
				throw new \Exception('Max tries exceeded.');
			}
			$tries++;
			sleep(pow(2, $tries));
		}

	}

}
