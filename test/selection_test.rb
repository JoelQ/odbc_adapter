require 'test_helper'

class SelectionTest < ODBCTest
  def test_first
    assert_equal 'Kevin', User.first.first_name
  end

  def test_pluck
    expected = %w[Ash Jason Kevin Michal Ryan Sharif]
    assert_equal expected, User.order(:first_name).pluck(:first_name)
  end

  def test_limitations
    expected = %w[Kevin Michal Ryan]
    assert_equal expected, User.order(:first_name).limit(3).offset(2).pluck(:first_name)
  end

  def test_find
    user = User.last
    assert_equal user, User.find(user.id)
  end
end
