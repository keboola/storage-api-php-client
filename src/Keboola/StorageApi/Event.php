<?php
/**
 *
 * User: Martin Halamíček
 * Date: 30.11.12
 * Time: 16:12
 *
 */

namespace Keboola\StorageApi;


class Event {

	/**
	 * Component name e.q. SFDC
	 * @var string
	 */
	private $_componentName;

	/**
	 * Component type e.q. Extractor
	 * @var string
	 */
	private $_componentType;

	/**
	 * Log message
	 * @var string
	 */
	private $_message;

	/**
	 * info | error
	 * @var string
	 */
	private $_type;

	/**
	 * Params associated to event e.q. sfdc configuration id
	 * @var array
	 */
	private $_params = array();

	/**
	 * Results associated to event, some performance metrics, fetched rows. couts etc.
	 * @var array
	 */
	private $_results = array();

	/**
	 * @var int run duration in seconds
	 */
	private $_duration;

	public function getComponentName()
	{
		return $this->_componentName;
	}


	/**
	 * @param $componentName
	 * @return Event
	 */
	public function setComponentName($componentName)
	{
		$this->_componentName = $componentName;
		return $this;
	}

	public function getComponentType()
	{
		return $this->_componentType;
	}

	/**
	 * @param $componentType
	 * @return Event
	 */
	public function setComponentType($componentType)
	{
		$this->_componentType = $componentType;
		return $this;
	}

	public function getMessage()
	{
		return $this->_message;
	}

	/**
	 * @param $message
	 * @return Event
	 */
	public function setMessage($message)
	{
		$this->_message = $message;
		return $this;
	}

	public function getType()
	{
		return $this->_type;
	}

	/**
	 * @param $type
	 * @return Event
	 */
	public function setType($type)
	{
		$allowedTypes = array(
			'info',
			'error',
		);
		if (!in_array($type, $allowedTypes)) {
			throw new Exception("{$type} is not allowed. Allowed types: " . implode(',', $allowedTypes));
		}

		$this->_type = $type;
		return $this;
	}

	public function getParams()
	{
		return $this->_params;
	}

	/**
	 * @param array $params
	 * @return Event
	 */
	public function setParams(array $params)
	{
		$this->_params = $params;
		return $this;
	}

	public function getResults()
	{
		return $this->_results;
	}

	/**
	 * @param $results
	 * @return Event
	 */
	public function setResults(array $results)
	{
		$this->_results = $results;
		return $this;
	}

	public function getDuration()
	{
		return $this->_duration;
	}

	/**
	 * @param $duration
	 * @return Event
	 */
	public function setDuration($duration)
	{
		$this->_duration = (int) $duration;
		return $this;
	}
}
