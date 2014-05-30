class BinlogEvent
  attr_reader :type, :attrs

  def initialize(type, attrs)
    @type = type
    @attrs = attrs
  end
end
