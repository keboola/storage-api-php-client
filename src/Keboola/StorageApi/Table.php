<?php
/**
 * Created by JetBrains PhpStorm.
 * User: Miro
 * Date: 25.9.12
 * Time: 12:53
 * To change this template use File | Settings | File Templates.
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
	protected $_data;

	/**
	 * key value pairs of attributes
	 *
	 * @var array
	 */
	protected $_attributes;

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

	/**
	 * @param array $data
	 * @param bool $header
	 */
	public function setFromArray($data, $hasHeader=false)
	{
		if ($hasHeader) {
			$this->_header = array_shift($data);
		}

		$this->_data = $data;
	}

	public function setFromString($string, $delimiter=',', $enclosure='"', $hasHeader=false)
	{
		$data = self::csvStringToArray($string, $delimiter, $enclosure);
		$this->setFromArray($data, $hasHeader);
	}

	public function setAttribute($key, $value)
	{
		$this->_attributes[$key] = $value;
	}


	/**
	 * Save data and table attributes to Storage API
	 *
	 * @throws TableException
	 */
	public function save()
	{
		$this->_preSave();

		$tempfile = tempnam(ROOT_PATH . "/tmp/", 'sapi-client-' . $this->_id . '-');
		$fh = fopen($tempfile, 'w+');

		if (!fputcsv($fh, $this->_header, ',', '"')) {
			throw new TableException('Error while writing header.');
		}

		foreach ($this->_data as $row) {
			if (!fputcsv($fh, $row, ',', '"')) {
				throw new TableException('Error while writing data.');
			}
		}

		if (!$this->_client->tableExists($this->_id)) {
			$this->_client->createTable($this->_bucketId, $this->_name, $tempfile);
		} else {
			$this->_client->writeTable($this->_id, $tempfile, null, ',', '"', true);
		}

		// Save table attributes
		foreach ($this->_attributes as $k => $v) {
			$this->_client->setTableAttribute($this->_id, $k, $v);
		}
	}

	protected function _preSave()
	{
		if (empty($this->_header)) {
			throw new TableException('Empty header');
		}

		if (empty($this->_data)) {
			throw new TableException('No data set');
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
			// Strip Double Quotes
			$row = str_replace('"""', '"', $row);

			if (!empty($row)) {
				$result[] = str_getcsv($row, $delimiter, $enclosure);
			}
		}

		return $result;
	}
}
