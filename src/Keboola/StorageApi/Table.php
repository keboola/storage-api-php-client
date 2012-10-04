<?php
/**
 * Storage API Client - Table abstraction
 *
 * Useful for data insertion to Storage API from array or string, without need to write temporary CSV files.
 * Temporary CSV file creation is handled by this class.
 *
 * @author Miroslav Cillik <miro@keboola.com>
 * @date: 25.9.12
 */

namespace Keboola\StorageApi;

class Table
{
	/**
	 * @var string
	 */
	protected $id;

	/**
	 * @var string
	 */
	protected $_name;

	/**
	 * @var string
	 */
	protected $_bucketId;

	/**
	 * @var Client
	 */
	protected $_client;

	/**
	 * Header columns array
	 *
	 * @var array
	 */
	protected $_header;

	/**
	 * 2 dimensional array of data - Rows x Columns
	 *
	 * @var array
	 */
	protected $_data = array();

	/**
	 * key value pairs of attributes
	 *
	 * @var array
	 */
	protected $_attributes = array();

	/**
	 * @param Client $client
	 * @param string $id - table ID
	 */
	public function __construct(Client $client, $id)
	{
		$this->_client = $client;
		$this->_id = $id;

		$tableNameArr = explode('.', $id);
		$this->_name = $tableNameArr[2];

		$bucketName = $tableNameArr[1];
		$stage = $tableNameArr[0];

		$this->_bucketId = $this->_client->getBucketId($bucketName, $stage);
	}

	/**
	 * @return string
	 */
	public function getId()
	{
		return $this->_id;
	}

	/**
	 * @return string
	 */
	public function getName()
	{
		return $this->_name;
	}

	/**
	 * @return string
	 */
	public function getBucketId()
	{
		return $this->_bucketId;
	}

	/**
	 * @return array
	 */
	public function getHeader()
	{
		return $this->_header;
	}

	/**
	 * @return array
	 */
	public function getData()
	{
		return $this->_data;
	}

	/**
	 * @param array $header
	 */
	public function setHeader($header)
	{
		$this->_header = self::normalizeHeader($header);
	}

	public function setAttribute($key, $value)
	{
		$this->_attributes[$key] = $value;
	}

	/**
	 * @param $key
	 * @return string
	 */
	public function getAttribute($key)
	{
		return $this->_attributes[$key];
	}

	/**
	 * @return array
	 */
	public function getAttributes()
	{
		return $this->_attributes;
	}

	/**
	 * @param array $data
	 * @param bool $header
	 */
	public function setFromArray($data, $hasHeader=false)
	{
		if (!is_array($this->_data)) {
			throw new TableException('Invalid data type - expected 2D Array');
		}

		if ($hasHeader) {
			$this->setHeader(array_shift($data));
		}

		$this->_data = $data;
	}

	public function setFromString($string, $delimiter=',', $enclosure='"', $hasHeader=false)
	{
		$data = self::csvStringToArray($string, $delimiter, $enclosure);
		$this->setFromArray($data, $hasHeader);
	}

	/**
	 * Save data and table attributes to Storage API
	 */
	public function save($incremental=false)
	{
		$this->_preSave();

		$tempfile = tempnam(ROOT_PATH . "/tmp/", 'sapi-client-' . $this->_id . '-');
		$file = new \Keboola\Csv\CsvFile($tempfile);
		$file->writeRow($this->_header);
		foreach ($this->_data as $row) {
			$file->writeRow($row);
		}

		if (!$this->_client->tableExists($this->_id)) {
			$this->_client->createTable($this->_bucketId, $this->_name, $tempfile);
		} else {
			$this->_client->writeTable($this->_id, $tempfile, null, ',', '"', $incremental);
		}

		// Save table attributes
		foreach ($this->_attributes as $k => $v) {
			$this->_client->setTableAttribute($this->_id, $k, $v);
		}
	}

	protected function _preSave()
	{
		if (empty($this->_header)) {
			throw new TableException('Empty header. Header must be set.');
		}
	}

	public static function normalizeHeader(&$header)
	{
		foreach($header as &$col) {
			$col = self::removeSpecialChars($col);
		}

		return $header;
	}

	public static function removeSpecialChars($string)
	{
		$string = str_replace('#', 'count', $string);
		$string = preg_replace("/[^A-Za-z0-9_\s]/", '', $string);
		$string = trim($string);
		$string = str_replace(' ', '_', $string);
		$string = lcfirst($string);

		if (!strlen($string)) {
			$string = 'empty';
		}

		return $string;
	}

	public static function csvStringToArray($string, $delimiter = ',', $enclosure = '"')
	{
		$result = array();
		$rows = explode("\n", $string);

		foreach($rows as $row) {
			if (!empty($row)) {
				$result[] = str_getcsv($row, $delimiter, $enclosure, $enclosure);
			}
		}

		return $result;
	}
}
