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
	protected $_client;

	public function __construct($client)
	{
		$this->_client = $client;
	}

	public function setFromArray($data, $header=false)
	{

	}
}
