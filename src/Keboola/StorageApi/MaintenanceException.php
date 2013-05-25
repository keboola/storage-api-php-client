<?php
namespace Keboola\StorageApi;

class MaintenanceException extends ClientException
{

	/**
	 * @var int
	 */
	private $_retryAfter;

	function __construct($reason, $retryAfter, $params)
	{
		$this->_retryAfter = (int) $retryAfter;
		parent::__construct($reason, 503, null, "MAINTENANCE", $params);
	}

	/**
	 * @return int
	 */
	public function getRetryAfter()
	{
		return $this->_retryAfter;
	}

}
