<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 16/09/14
 * Time: 01:50
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\StorageApi\Options\Components;


class ListConfigurationsOptions
{
	private $componentType;

	/**
	 * @return mixed
	 */
	public function getComponentType()
	{
		return $this->componentType;
	}

	/**
	 * @param mixed $componentType
	 */
	public function setComponentType($componentType)
	{
		$this->componentType = $componentType;
		return $this;
	}

	public function toParamsArray()
	{
		return array(
			'componentType' => $this->getComponentType(),
		);
	}

}