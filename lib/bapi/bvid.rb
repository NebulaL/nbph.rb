# frozen_string_literal: true

class BVID
  @@table = 'fZodR9XQDSUm21yCkr6zBqiveYah8bt4xsWpHnJE7jL5VG3guMTKNPAwcF'
  @@tr = { 'f' => 0, 'Z' => 1, 'o' => 2, 'd' => 3, 'R' => 4, '9' => 5, 'X' => 6,
           'Q' => 7, 'D' => 8, 'S' => 9, 'U' => 10, 'm' => 11, '2' => 12, '1' => 13,
           'y' => 14, 'C' => 15, 'k' => 16, 'r' => 17, '6' => 18, 'z' => 19, 'B' => 20,
           'q' => 21, 'i' => 22, 'v' => 23, 'e' => 24, 'Y' => 25, 'a' => 26, 'h' => 27,
           '8' => 28, 'b' => 29, 't' => 30, '4' => 31, 'x' => 32, 's' => 33, 'W' => 34,
           'p' => 35, 'H' => 36, 'n' => 37, 'J' => 38, 'E' => 39, '7' => 40, 'j' => 41,
           'L' => 42, '5' => 43, 'V' => 44, 'G' => 45, '3' => 46, 'g' => 47, 'u' => 48,
           'M' => 49, 'T' => 50, 'K' => 51, 'N' => 52, 'P' => 53, 'A' => 54, 'w' => 55,
           'c' => 56, 'F' => 57 }
  @@s = [11, 10, 3, 8, 4, 6]
  @@xor = 177_451_812
  @@add = 8_728_348_608

  def self.to_aid(bid)
    r = 0
    (0..5).each do |i|
      r += @@tr[bid[@@s[i]]] * 58**i
    end
    (r - @@add) ^ @@xor
  end

  def self.to_bvid(aid)
    aid = (aid ^ @@xor) + @@add
    r = ['B', 'V', '1', ' ', ' ', '4', ' ', '1', ' ', '7', ' ', ' ']
    (0..5).each do |i|
      r[@@s[i]] = @@table[aid / 58**i % 58]
    end
    r.join
  end

  # 作者：mcfx
  # 链接：https://www.zhihu.com/question/381784377/answer/1099438784
  # 来源：知乎
  # 著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。

  # translate to ruby: NebulaL

  def self.type(vid)
    case vid
    when Integer
      return %i[aid num] if vid.positive?
    when String
      case vid
      when /^[Aa][Vv]\d+$/
        return %i[aid str_with_prefix]
      when /^\d+$/
        return %i[aid str]
      when /^[Bb][Vv]1[a-zA-Z0-9]{2,2}4[a-zA-Z0-9]{3,3}7[a-zA-Z0-9]{2,3}$/
        return %i[bvid str_with_prefix]
      when /^1[a-zA-Z0-9]{2,2}4[a-zA-Z0-9]{3,3}7[a-zA-Z0-9]{2,3}$/
        return %i[bvid str]
      end
    end
    %i[unknown unknown]
  end

  def self.aid?(vid)
    type(vid).first == :aid
  end

  def self.bvid?(vid)
    type(vid).first == :bvid
  end

  def self.format(vid, type = nil)
    vid_type, vid_format = type(vid)

    if type.is_a? Symbol
      return format(vid, nil) if type == vid_type

      case vid_type
      when :aid
        case type
        when :bvid
          to_bvid(format(vid, nil))
        else
          raise TypeError, "can not convert #{vid} to type #{type}"
        end
      when :bvid
        case type
        when :aid
          to_aid(format(vid, nil))
        else
          raise TypeError, "can not convert #{vid} to type #{type}"
        end
      end
    elsif type
      raise TypeError, 'type should be a symbol'
    end

    case vid_type
    when :aid
      case vid_format
      when :num
        vid
      when :str
        vid.to_i
      when :str_with_prefix
        vid.sub(/[Aa][Vv]/, '').to_i
      end
    when :bvid
      case vid_format
      when :str_with_prefix
        vid.sub(/[Bb][Vv]/, 'BV')
      when :str
        "BV#{vid}"
      end
    else
      raise TypeError, "can not detect #{vid}'s type"
    end
  end
end
