import random
import time
import os
from datetime import datetime

class TicTacToe:
    """井字棋游戏（支持人机对战）"""
    
    def __init__(self):
        self.board = [' '] * 9
        self.current_player = 'X'
        self.game_mode = None  # 'PVP' = 玩家对玩家, 'PVC' = 玩家对电脑
        
    def display_board(self):
        """显示棋盘"""
        print("\n   |   |   ")
        print(f" {self.board[0]} | {self.board[1]} | {self.board[2]} ")
        print("___|___|___")
        print("   |   |   ")
        print(f" {self.board[3]} | {self.board[4]} | {self.board[5]} ")
        print("___|___|___")
        print("   |   |   ")
        print(f" {self.board[6]} | {self.board[7]} | {self.board[8]} ")
        print("   |   |   ")
        
        # 显示位置编号
        print("\n位置编号:")
        print("   |   |   ")
        print(" 1 | 2 | 3 ")
        print("___|___|___")
        print("   |   |   ")
        print(" 4 | 5 | 6 ")
        print("___|___|___")
        print("   |   |   ")
        print(" 7 | 8 | 9 ")
        print("   |   |   ")
        
    def make_move(self, position):
        """落子"""
        if self.board[position - 1] == ' ':
            self.board[position - 1] = self.current_player
            return True
        return False
        
    def check_winner(self):
        """检查获胜者"""
        # 所有可能的获胜组合
        winning_combinations = [
            [0, 1, 2], [3, 4, 5], [6, 7, 8],  # 行
            [0, 3, 6], [1, 4, 7], [2, 5, 8],  # 列
            [0, 4, 8], [2, 4, 6]              # 对角线
        ]
        
        for combo in winning_combinations:
            if (self.board[combo[0]] == self.board[combo[1]] == 
                self.board[combo[2]] != ' '):
                return self.board[combo[0]]
                
        # 检查是否平局
        if ' ' not in self.board:
            return 'Tie'
            
        return None
        
    def switch_player(self):
        """切换玩家"""
        self.current_player = 'O' if self.current_player == 'X' else 'X'
        
    def get_empty_positions(self):
        """获取所有空位置"""
        return [i + 1 for i, cell in enumerate(self.board) if cell == ' ']
        
    def ai_move(self):
        """电脑AI移动 - 使用简单策略"""
        empty_positions = self.get_empty_positions()
        
        # 1. 检查是否能获胜
        for pos in empty_positions:
            self.board[pos - 1] = 'O'
            if self.check_winner() == 'O':
                self.board[pos - 1] = ' '
                return pos
            self.board[pos - 1] = ' '
            
        # 2. 检查是否需要阻止玩家获胜
        for pos in empty_positions:
            self.board[pos - 1] = 'X'
            if self.check_winner() == 'X':
                self.board[pos - 1] = ' '
                return pos
            self.board[pos - 1] = ' '
            
        # 3. 优先选择中心位置
        if 5 in empty_positions:
            return 5
            
        # 4. 优先选择角落位置
        corners = [1, 3, 7, 9]
        available_corners = [pos for pos in corners if pos in empty_positions]
        if available_corners:
            return random.choice(available_corners)
            
        # 5. 随机选择边位置
        return random.choice(empty_positions)
        
    def select_game_mode(self):
        """选择游戏模式"""
        print("\n请选择游戏模式:")
        print("1. 玩家 vs 玩家")
        print("2. 玩家 vs 电脑")
        
        while True:
            choice = input("请输入选择 (1-2): ").strip()
            if choice == '1':
                self.game_mode = 'PVP'
                return
            elif choice == '2':
                self.game_mode = 'PVC'
                return
            else:
                print("❌ 请输入有效选项 (1-2)")
                
    def get_player_move(self):
        """获取玩家移动"""
        while True:
            try:
                position = int(input(f"玩家 {self.current_player}，请选择位置 (1-9): "))
                if position < 1 or position > 9:
                    print("❌ 请输入1-9之间的数字！")
                    continue
                    
                if not self.make_move(position):
                    print("❌ 该位置已被占用，请选择其他位置！")
                    continue
                    
                return True
                
            except ValueError:
                print("❌ 请输入有效的数字！")
                
    def play(self):
        """开始游戏"""
        print("=" * 50)
        print("🎮 欢迎来到井字棋游戏！")
        print("=" * 50)
        
        self.select_game_mode()
        
        if self.game_mode == 'PVC':
            print("\n你将扮演 X，电脑扮演 O")
            print("难度: 简单AI")
        
        while True:
            self.display_board()
            
            # 玩家回合
            if self.current_player == 'X':
                self.get_player_move()
            # 电脑回合
            elif self.current_player == 'O' and self.game_mode == 'PVC':
                print(f"\n电脑 {self.current_player} 正在思考...")
                time.sleep(1)  # 模拟思考时间
                ai_position = self.ai_move()
                self.make_move(ai_position)
                print(f"电脑选择了位置 {ai_position}")
            # 玩家2回合（PVP模式）
            else:
                self.get_player_move()
                
            winner = self.check_winner()
            if winner:
                self.display_board()
                if winner == 'Tie':
                    print("\n🤝 平局！")
                elif winner == 'X':
                    if self.game_mode == 'PVC':
                        print("\n🎉 恭喜你获胜！")
                    else:
                        print(f"\n🎉 玩家 X 获胜！")
                else:  # winner == 'O'
                    if self.game_mode == 'PVC':
                        print("\n💻 电脑获胜！")
                    else:
                        print(f"\n🎉 玩家 O 获胜！")
                break
                
            self.switch_player()
                
        # 询问是否再玩一次
        if input("\n🔄 想再玩一次吗？(y/n): ").lower() in ['y', 'yes']:
            self.__init__()  # 重置游戏
            self.play()

class Game2048:
    """2048游戏"""
    
    def __init__(self):
        self.size = 4
        self.board = [[0] * self.size for _ in range(self.size)]
        self.score = 0
        self.add_new_tile()
        self.add_new_tile()
        
    def display_board(self):
        """显示游戏板"""
        print(f"\n得分: {self.score}")
        print("+" + "------+" * self.size)
        
        for row in self.board:
            print("|", end="")
            for cell in row:
                if cell == 0:
                    print("      |", end="")
                else:
                    print(f"{cell:^6}|", end="")
            print()
            print("+" + "------+" * self.size)
            
    def add_new_tile(self):
        """添加新的数字块"""
        empty_cells = []
        for i in range(self.size):
            for j in range(self.size):
                if self.board[i][j] == 0:
                    empty_cells.append((i, j))
                    
        if empty_cells:
            i, j = random.choice(empty_cells)
            self.board[i][j] = 2 if random.random() < 0.9 else 4
            
    def move_left(self):
        """向左移动"""
        moved = False
        for i in range(self.size):
            # 移除空格并合并相同数字
            row = [x for x in self.board[i] if x != 0]
            for j in range(len(row) - 1):
                if row[j] == row[j + 1]:
                    row[j] *= 2
                    self.score += row[j]
                    row[j + 1] = 0
            row = [x for x in row if x != 0]
            # 填充剩余空间
            while len(row) < self.size:
                row.append(0)
            # 检查是否有变化
            if row != self.board[i]:
                moved = True
            self.board[i] = row
        return moved
        
    def move_right(self):
        """向右移动"""
        # 反转每行，向左移动，再反转回来
        for i in range(self.size):
            self.board[i] = self.board[i][::-1]
        moved = self.move_left()
        for i in range(self.size):
            self.board[i] = self.board[i][::-1]
        return moved
        
    def move_up(self):
        """向上移动"""
        # 转置矩阵，向左移动，再转置回来
        self.transpose()
        moved = self.move_left()
        self.transpose()
        return moved
        
    def move_down(self):
        """向下移动"""
        # 转置矩阵，向右移动，再转置回来
        self.transpose()
        moved = self.move_right()
        self.transpose()
        return moved
        
    def transpose(self):
        """转置矩阵"""
        self.board = [list(row) for row in zip(*self.board)]
        
    def is_game_over(self):
        """检查游戏是否结束"""
        # 检查是否还有空格
        for i in range(self.size):
            for j in range(self.size):
                if self.board[i][j] == 0:
                    return False
                    
        # 检查是否还能合并
        for i in range(self.size):
            for j in range(self.size):
                if (i < self.size - 1 and self.board[i][j] == self.board[i + 1][j]) or \
                   (j < self.size - 1 and self.board[i][j] == self.board[i][j + 1]):
                    return False
                    
        return True
        
    def has_won(self):
        """检查是否获胜（达到2048）"""
        for i in range(self.size):
            for j in range(self.size):
                if self.board[i][j] == 2048:
                    return True
        return False
        
    def play(self):
        """开始游戏"""
        print("=" * 50)
        print("🎮 欢迎来到2048游戏！")
        print("使用 W/A/S/D 或方向键控制方块移动")
        print("目标：合并方块达到2048！")
        print("=" * 50)
        
        while True:
            self.display_board()
            
            if self.has_won():
                print("\n🎉 恭喜！你达到了2048！")
                if input("继续游戏吗？(y/n): ").lower() not in ['y', 'yes']:
                    break
                    
            if self.is_game_over():
                print("\n😔 游戏结束！")
                print(f"最终得分: {self.score}")
                break
                
            move = input(": ").upper()
            
            moved = False
            if move == 'W':
                moved = self.move_up()
            elif move == 'S':
                moved = self.move_down()
            elif move == 'A':
                moved = self.move_left()
            elif move == 'D':
                moved = self.move_right()
            else:
                print("❌ 请输入有效的方向键 (W/A/S/D)！")
                continue
                
            if moved:
                self.add_new_tile()
            else:
                print("❌ 该方向无法移动！")

class TypingPractice:
    """打字练习游戏"""
    
    def __init__(self):
        self.words = [
            "python", "programming", "computer", "keyboard", "screen",
            "practice", "exercise", "challenge", "learning", "development",
            "algorithm", "function", "variable", "string", "integer",
            "boolean", "database", "network", "internet", "website",
            "application", "software", "hardware", "memory", "processor",
            "interface", "design", "creative", "solution", "problem"
        ]
        
        self.sentences = [
            "Practice makes perfect",
            "The quick brown fox jumps over the lazy dog",
            "Python is a powerful programming language",
            "Typing speed improves with regular practice",
            "Accuracy is more important than speed",
            "Consistent practice leads to improvement",
            "Focus on proper finger placement",
            "Keep your eyes on the screen not the keyboard",
            "Start slow and gradually increase speed",
            "Mistakes are part of the learning process"
        ]
        
    def calculate_wpm(self, text, time_taken):
        """计算每分钟单词数"""
        words = len(text.split())
        minutes = time_taken / 60
        return round(words / minutes) if minutes > 0 else 0
        
    def calculate_accuracy(self, original, typed):
        """计算准确率"""
        if len(original) == 0:
            return 100.0
            
        correct = 0
        for i, char in enumerate(typed):
            if i < len(original) and char == original[i]:
                correct += 1
                
        return round((correct / len(original)) * 100, 2)
        
    def word_practice(self):
        """单词练习模式"""
        print("\n🔤 单词练习模式")
        print("输入显示的单词，按 Enter 提交")
        print("输入 'quit' 退出练习")
        
        total_words = 0
        correct_words = 0
        total_time = 0
        
        start_time = time.time()
        
        while True:
            word = random.choice(self.words)
            print(f"\n请输入单词: {word}")
            
            user_input = input().strip()
            
            if user_input.lower() == 'quit':
                break
                
            total_words += 1
            if user_input == word:
                correct_words += 1
                print("✅ 正确！")
            else:
                print(f"❌ 错误！正确答案是: {word}")
                
        end_time = time.time()
        total_time = end_time - start_time
        
        if total_words > 0:
            accuracy = (correct_words / total_words) * 100
            wpm = self.calculate_wpm(' '.join(self.words[:total_words]), total_time)
            
            print(f"\n📊 练习结果:")
            print(f"总单词数: {total_words}")
            print(f"正确单词数: {correct_words}")
            print(f"准确率: {accuracy:.1f}%")
            print(f"打字速度: {wpm} WPM")
            print(f"用时: {total_time:.2f} 秒")
            
    def sentence_practice(self):
        """句子练习模式"""
        print("\n📝 句子练习模式")
        print("输入显示的句子，按 Enter 提交")
        print("输入 'quit' 退出练习")
        
        total_sentences = 0
        total_chars = 0
        correct_chars = 0
        total_time = 0
        
        start_time = time.time()
        
        while True:
            sentence = random.choice(self.sentences)
            print(f"\n请输入句子: {sentence}")
            
            user_input = input().strip()
            
            if user_input.lower() == 'quit':
                break
                
            total_sentences += 1
            total_chars += len(sentence)
            accuracy = self.calculate_accuracy(sentence, user_input)
            correct_chars += int(len(sentence) * accuracy / 100)
            
            print(f"准确率: {accuracy}%")
            
        end_time = time.time()
        total_time = end_time - start_time
        
        if total_sentences > 0:
            overall_accuracy = (correct_chars / total_chars) * 100 if total_chars > 0 else 0
            wpm = self.calculate_wpm(' '.join(self.sentences[:total_sentences]), total_time)
            
            print(f"\n📊 练习结果:")
            print(f"总句子数: {total_sentences}")
            print(f"总字符数: {total_chars}")
            print(f"正确字符数: {correct_chars}")
            print(f"整体准确率: {overall_accuracy:.1f}%")
            print(f"打字速度: {wpm} WPM")
            print(f"用时: {total_time:.2f} 秒")
            
    def timed_challenge(self):
        """计时挑战模式"""
        print("\n⏱️ 计时挑战模式")
        print("在30秒内尽可能多地正确输入单词")
        print("准备好了吗？按 Enter 开始...")
        input()
        
        start_time = time.time()
        end_time = start_time + 30  # 30秒挑战
        correct_words = 0
        total_words = 0
        
        print("开始！")
        
        while time.time() < end_time:
            remaining_time = max(0, end_time - time.time())
            if remaining_time <= 0:
                break
                
            word = random.choice(self.words)
            print(f"\n[{remaining_time:.1f}s] 请输入: {word}")
            
            user_input = input().strip()
            total_words += 1
            
            if user_input == word:
                correct_words += 1
                print("✅")
            else:
                print("❌")
                
        print(f"\n⏰ 时间到！")
        print(f"挑战结果:")
        print(f"总单词数: {total_words}")
        print(f"正确单词数: {correct_words}")
        if total_words > 0:
            accuracy = (correct_words / total_words) * 100
            print(f"准确率: {accuracy:.1f}%")
            
    def play(self):
        """开始打字练习"""
        print("=" * 50)
        print("⌨️  欢迎来到打字练习游戏！")
        print("提高你的打字速度和准确性")
        print("=" * 50)
        
        while True:
            print("\n请选择练习模式:")
            print("1. 单词练习")
            print("2. 句子练习")
            print("3. 计时挑战 (30秒)")
            print("0. 返回主菜单")
            
            choice = input("\n请输入选择 (0-3): ").strip()
            
            if choice == '1':
                self.word_practice()
            elif choice == '2':
                self.sentence_practice()
            elif choice == '3':
                self.timed_challenge()
            elif choice == '0':
                break
            else:
                print("❌ 请输入有效选项 (0-3)")

def main_menu():
    """主菜单"""
    games = {
        '1': ('井字棋', TicTacToe),
        '2': ('2048', Game2048),
        '3': ('打字练习', TypingPractice),
        '0': ('退出', None)
    }
    
    while True:
        print("\n" + "=" * 50)
        print("🎮 休闲小游戏合集")
        print("=" * 50)
        
        for key, (name, _) in games.items():
            print(f"{key}. {name}")
            
        choice = input("\n请选择游戏 (0-3): ").strip()
        
        if choice in games:
            if choice == '0':
                print("👋 再见！祝你玩得开心！")
                break
            else:
                print(f"\n🎮 正在启动: {games[choice][0]}")
                time.sleep(1)
                game = games[choice][1]()
                game.play()
        else:
            print("❌ 无效选择，请输入 0-3 之间的数字")

if __name__ == "__main__":
    main_menu()