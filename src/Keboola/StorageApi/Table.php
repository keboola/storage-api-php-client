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
	 * @param Client $client
	 * @param string $id - table ID
	 */
	public function __construct(Client $client, string $id)
	{
		$this->_client = $client;
		$this->_id = $id;

		$tableNameArr = explode('.', $id);
		$this->_name = $tableNameArr[2];
		$this->_bucketId = $tableNameArr[0] . '.' . $tableNameArr[1];
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
	public function setHeader(array $header)
	{
		$this->_header = $header;
	}

	/**
	 * @param array $data
	 * @param bool $header
	 */
	public function setFromArray(array $data, $header=false)
	{
		if ($header) {
			$this->_header = array_shift($data);
		}

		$this->_data = $data;
	}


	public function save()
	{
		$this->_preSave();

		$tempfile = tempnam(ROOT_PATH . "/tmp/", 'sapi-client-' . $this->_id . '-');
		$fh = fopen($tempfile, 'w+');

		if (!fputcsv($fh, $this->_header, ',', '"')) {
			throw new TableException('Error while writing header.');
		}
		if (!fputcsv($fh, $this->_data, ',', '"')) {
			throw new TableException('Error while writing data.');
		}

		if (!$this->_client->tableExists($this->_id)) {
			$this->_client->createTable($this->_bucketId, $this->_name, $tempfile);
		} else {
			$this->_client->writeTable($this->_id, $tempfile, null, ',', '"', true);
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
}
