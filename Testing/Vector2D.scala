
// No comments other than this. Because this is math codez
class Vector2D(val x: Double, val y: Double) {
  def +(other: Vector2D): Vector2D = {
    new Vector2D(other.x + x, other.y + y)
  }
  def -(other: Vector2D): Vector2D ={
    new Vector2D(other.x - x, other.y - y)
  }
  def *(other: Vector2D): Double ={
    x * other.x + y * other.y
  }
  def scalar_mul(other: Double): Double ={
    new Vector2D(x * other, y * other)
  }
  def div(other: Double):  = {
    new Vector2D(x /  other, y / other)
  }

}